//! Atomic batches of queries.
//!
//! D1 doesn't support interactive transactions, where a query's rows are read
//! before deciding on the next query. Instead, it can run a list of statements
//! as a single transaction with
//! [`batch()`](https://developers.cloudflare.com/d1/worker-api/d1-database/#batch):
//! the statements run one after the other, and if one fails, all of them are
//! rolled back.
//!
//! A [`Batch`] is such a list of statements, along with how to decode each
//! statement's result. Every statement is known before the batch runs, so a
//! statement can't depend on another one's result in Rust. Values can still flow
//! between statements in SQL, e.g. with `last_insert_rowid()` or subqueries.
//!
//! ```ignore
//! let (user_id, _) = conn
//!     .batch((
//!         sqlx_d1::batch::fetch_one(sqlx_d1::query_scalar!(
//!             "INSERT INTO users (name) VALUES (?) RETURNING id",
//!             name,
//!         )),
//!         sqlx_d1::batch::execute(sqlx_d1::query!(
//!             "INSERT INTO emails (user_id, email) VALUES (last_insert_rowid(), ?)",
//!             email,
//!         )),
//!     ))
//!     .await?;
//! ```

use std::collections::VecDeque;

use sqlx_core::arguments::IntoArguments;
use sqlx_core::error::BoxDynError;
use sqlx_core::executor::Execute;
use sqlx_core::from_row::FromRow;
use sqlx_core::query::{Map, Query};
use sqlx_core::query_as::QueryAs;
use sqlx_core::query_scalar::QueryScalar;

use crate::arguments::D1Arguments;
use crate::query_result::D1QueryResult;
use crate::row::D1Row;
use crate::{D1, ResultFuture};

/// Statements that run as a single transaction, and decode into a `T`.
///
/// A batch is created from a query with [`execute`], [`fetch_one`],
/// [`fetch_optional`] or [`fetch_all`], combined with other batches with
/// [`zip`](Self::zip) or by collecting an iterator of batches, and run with
/// [`D1Connection::batch`](crate::D1Connection::batch).
#[must_use = "a batch does nothing until it's run with `D1Connection::batch`"]
pub struct Batch<'q, T> {
    statements: Vec<Statement<'q>>,
    decode: Decode<'q, T>,
}

/// A type that can be turned into a [`Batch`], i.e. a batch itself, or a tuple
/// of batches.
pub trait IntoBatch<'q> {
    /// The type the batch decodes into.
    type Output;

    /// Returns the batch.
    fn into_batch(self) -> Batch<'q, Self::Output>;
}

/// A query whose rows can be fetched in a [`Batch`].
///
/// It's implemented for the query types the query functions and macros return.
pub trait BatchQuery<'q>: sealed::DecodeRows<'q> {}

/// A statement of a batch.
#[cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]
struct Statement<'q> {
    sql: &'q str,
    arguments: Result<Option<D1Arguments>, BoxDynError>,
}

/// The function decoding a batch from the results of its statements.
///
/// It removes the results of the batch's statements from the front of the
/// queue before returning, so that the decoders of combined batches each take
/// their own statements' results.
type Decode<'q, T> = Box<dyn FnOnce(&mut Results) -> ResultFuture<'q, T> + Send + 'q>;

/// The results of a batch's statements that haven't been decoded yet, in the
/// statements' order.
pub(crate) type Results = VecDeque<StatementResult>;

mod sealed {
    use super::*;

    /// The result of a statement of a batch.
    #[derive(Default)]
    pub struct StatementResult {
        pub(crate) rows: Vec<D1Row>,
        pub(crate) query_result: D1QueryResult,
    }

    /// The decoding of a query's rows, shared by the query types.
    pub trait DecodeRows<'q>: Execute<'q, D1> + Send + Sized + 'q {
        /// The type each row is decoded into.
        type Output: Send + 'q;

        fn decode_all(self, result: StatementResult) -> ResultFuture<'q, Vec<Self::Output>>;

        fn decode_one(self, result: StatementResult) -> ResultFuture<'q, Self::Output>;

        fn decode_optional(self, result: StatementResult)
        -> ResultFuture<'q, Option<Self::Output>>;
    }
}

pub(crate) use sealed::StatementResult;

/// Returns a batch running the query, which decodes into the query's result,
/// e.g. the number of rows it changed.
pub fn execute<'q, Q>(query: Q) -> Batch<'q, D1QueryResult>
where
    Q: Execute<'q, D1> + Send + 'q,
{
    Batch::single(query, |_query, result| {
        Box::pin(async move { Ok(result.query_result) })
    })
}

/// Returns a batch running the query, which decodes into all its rows.
pub fn fetch_all<'q, Q: BatchQuery<'q>>(query: Q) -> Batch<'q, Vec<Q::Output>> {
    Batch::single(query, Q::decode_all)
}

/// Returns a batch running the query, which decodes into its first row.
///
/// Running the batch fails with [`Error::RowNotFound`](sqlx_core::Error::RowNotFound)
/// if the query returns no rows, after all of the batch's statements have been
/// committed.
pub fn fetch_one<'q, Q: BatchQuery<'q>>(query: Q) -> Batch<'q, Q::Output> {
    Batch::single(query, Q::decode_one)
}

/// Returns a batch running the query, which decodes into its first row, if it
/// returns any.
pub fn fetch_optional<'q, Q: BatchQuery<'q>>(query: Q) -> Batch<'q, Option<Q::Output>> {
    Batch::single(query, Q::decode_optional)
}

impl<'q, T: Send + 'q> Batch<'q, T> {
    /// Returns a batch running the same statements, which decodes into the
    /// result of calling `f` on what this batch decodes into.
    pub fn map<U: 'q>(self, f: impl FnOnce(T) -> U + Send + 'q) -> Batch<'q, U> {
        let decode = self.decode;

        Batch {
            statements: self.statements,
            decode: Box::new(move |results| {
                let decoded = decode(results);
                Box::pin(async move { decoded.await.map(f) })
            }),
        }
    }

    /// Returns a batch running this batch's statements followed by `other`'s,
    /// which decodes into what both batches decode into.
    pub fn zip<U: Send + 'q>(self, other: Batch<'q, U>) -> Batch<'q, (T, U)> {
        let mut statements = self.statements;
        statements.extend(other.statements);

        let (decode_self, decode_other) = (self.decode, other.decode);

        Batch {
            statements,
            decode: Box::new(move |results| {
                let decoded_self = decode_self(results);
                let decoded_other = decode_other(results);
                Box::pin(async move { Ok((decoded_self.await?, decoded_other.await?)) })
            }),
        }
    }
}

impl<'q, T> Batch<'q, T> {
    /// Returns a batch running a single query, whose result is decoded with
    /// `decode`.
    fn single<Q>(
        mut query: Q,
        decode: impl FnOnce(Q, StatementResult) -> ResultFuture<'q, T> + Send + 'q,
    ) -> Self
    where
        Q: Execute<'q, D1> + Send + 'q,
    {
        let statement = Statement {
            sql: query.sql(),
            arguments: query.take_arguments(),
        };

        Self {
            statements: vec![statement],
            decode: Box::new(move |results| match results.pop_front() {
                Some(result) => decode(query, result),
                None => Box::pin(async { Err(missing_result()) }),
            }),
        }
    }

    /// Splits the batch into its statements, as their SQL and arguments, and
    /// its decoder.
    ///
    /// Fails with the error of the first statement whose arguments couldn't be
    /// encoded.
    #[allow(clippy::type_complexity)]
    #[cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]
    fn into_parts(
        self,
    ) -> Result<(Vec<(&'q str, Option<D1Arguments>)>, Decode<'q, T>), BoxDynError> {
        let statements = self
            .statements
            .into_iter()
            .map(|statement| Ok((statement.sql, statement.arguments?)))
            .collect::<Result<_, BoxDynError>>()?;

        Ok((statements, self.decode))
    }
}

impl crate::D1Connection {
    /// Runs the batch's statements as a single transaction, and returns what
    /// the batch decodes into.
    ///
    /// The statements run in order. If one of them fails, none of them are
    /// committed.
    pub fn batch<'q, B>(&self, batch: B) -> ResultFuture<'q, B::Output>
    where
        B: IntoBatch<'q>,
        B::Output: 'q,
    {
        #[cfg(not(target_arch = "wasm32"))]
        {
            let _ = batch;
            panic!(
                "native `D1Connection::batch`: Invalid use of `sqlx_d1`. Be sure to use \
                 `sqlx_d1` where the target is set to `wasm32-unknown-unknown` !"
            )
        }
        #[cfg(target_arch = "wasm32")]
        {
            let (statements, decode) = match batch.into_batch().into_parts() {
                Ok(parts) => parts,
                Err(err) => return Box::pin(async { Err(sqlx_core::Error::Encode(err)) }),
            };

            let d1 = self.inner.clone();

            Box::pin(worker::send::SendFuture::new(async move {
                let mut results = if statements.is_empty() {
                    Results::new()
                } else {
                    run(&d1, statements).await?
                };

                decode(&mut results).await
            }))
        }
    }
}

/// Runs the statements with D1's `batch()`, and returns their results.
#[cfg(target_arch = "wasm32")]
async fn run(
    d1: &worker_sys::D1Database,
    statements: Vec<(&str, Option<D1Arguments>)>,
) -> Result<Results, sqlx_core::Error> {
    use crate::error::D1Error;
    use worker::js_sys::{Array, Reflect};
    use worker::wasm_bindgen::{JsCast, JsValue};
    use worker::wasm_bindgen_futures::JsFuture;

    let prepared = Array::new();

    for (sql, arguments) in statements {
        let mut statement = d1.prepare(sql).map_err(D1Error::from)?;
        if let Some(arguments) = arguments {
            statement = statement
                .bind(arguments.as_ref().iter().collect())
                .map_err(|e| sqlx_core::Error::Encode(Box::new(D1Error::from(e))))?;
        }
        prepared.push(&statement);
    }

    let raw_results = JsFuture::from(d1.batch(prepared).map_err(D1Error::from)?)
        .await
        .map_err(D1Error::from)?
        .unchecked_into::<Array>();

    raw_results
        .iter()
        .map(|raw_result| {
            let result = worker_sys::D1Result::from(raw_result);

            let rows = result
                .results()
                .map_err(D1Error::from)?
                .map(|rows| rows.iter().map(D1Row::from_raw).collect())
                .transpose()?
                .unwrap_or_default();

            let meta = result.meta().map_err(D1Error::from)?;
            let meta_number = |key: &str| -> Result<f64, sqlx_core::Error> {
                Ok(Reflect::get(&meta, &JsValue::from_str(key))
                    .map_err(D1Error::from)?
                    .as_f64()
                    .unwrap_or_default())
            };

            Ok(StatementResult {
                rows,
                query_result: D1QueryResult {
                    rows_affected: meta_number("changes")? as usize,
                    last_insert_row_id: meta_number("last_row_id")? as i64,
                },
            })
        })
        .collect()
}

impl<'q, T: Send + 'q> FromIterator<Batch<'q, T>> for Batch<'q, Vec<T>> {
    /// Returns a batch running the batches' statements in order, which decodes
    /// into what each batch decodes into.
    fn from_iter<I: IntoIterator<Item = Batch<'q, T>>>(batches: I) -> Self {
        let mut statements = Vec::new();
        let mut decoders = Vec::new();

        for batch in batches {
            statements.extend(batch.statements);
            decoders.push(batch.decode);
        }

        Self {
            statements,
            decode: Box::new(move |results| {
                let decoded = decoders
                    .into_iter()
                    .map(|decode| decode(results))
                    .collect::<Vec<_>>();

                Box::pin(async move {
                    let mut outputs = Vec::with_capacity(decoded.len());
                    for decoded in decoded {
                        outputs.push(decoded.await?);
                    }
                    Ok(outputs)
                })
            }),
        }
    }
}

impl<'q, T> IntoBatch<'q> for Batch<'q, T> {
    type Output = T;

    fn into_batch(self) -> Batch<'q, T> {
        self
    }
}

// Each batch's output is its own type parameter, rather than a projection like
// `<A as IntoBatch<'q>>::Output`, because rustc can't prove that an async block
// holding such projections is `Send`.
macro_rules! impl_into_batch_for_tuples {
    ($( ($($batch:ident => $output:ident),+) )+) => {$(
        impl<'q, $($batch, $output),+> IntoBatch<'q> for ($($batch,)+)
        where
            $($batch: IntoBatch<'q, Output = $output>, $output: Send + 'q,)+
        {
            type Output = ($($output,)+);

            #[allow(non_snake_case)]
            fn into_batch(self) -> Batch<'q, Self::Output> {
                let ($($batch,)+) = self;
                let ($($batch,)+) = ($($batch.into_batch(),)+);

                let mut statements = Vec::new();
                $(statements.extend($batch.statements);)+

                let ($($batch,)+) = ($($batch.decode,)+);

                Batch {
                    statements,
                    decode: Box::new(move |results| {
                        let ($($batch,)+) = ($($batch(results),)+);
                        Box::pin(async move { Ok(($($batch.await?,)+)) })
                    }),
                }
            }
        }
    )+};
}

impl_into_batch_for_tuples! {
    (A => OA)
    (A => OA, B => OB)
    (A => OA, B => OB, C => OC)
    (A => OA, B => OB, C => OC, D => OD)
    (A => OA, B => OB, C => OC, D => OD, E => OE)
    (A => OA, B => OB, C => OC, D => OD, E => OE, F => OF)
    (A => OA, B => OB, C => OC, D => OD, E => OE, F => OF, G => OG)
    (A => OA, B => OB, C => OC, D => OD, E => OE, F => OF, G => OG, H => OH)
}

impl<'q, A> BatchQuery<'q> for Query<'q, D1, A> where A: IntoArguments<'q, D1> + Send + 'q {}

impl<'q, O, A> BatchQuery<'q> for QueryAs<'q, D1, O, A>
where
    A: IntoArguments<'q, D1> + Send + 'q,
    O: for<'r> FromRow<'r, D1Row> + Send + Unpin + 'q,
{
}

impl<'q, O, A> BatchQuery<'q> for QueryScalar<'q, D1, O, A>
where
    A: IntoArguments<'q, D1> + Send + 'q,
    O: Send + Unpin + 'q,
    (O,): for<'r> FromRow<'r, D1Row> + Send + Unpin,
{
}

impl<'q, F, O, A> BatchQuery<'q> for Map<'q, D1, F, A>
where
    F: FnMut(D1Row) -> Result<O, sqlx_core::Error> + Send + 'q,
    O: Send + Unpin + 'q,
    A: IntoArguments<'q, D1> + Send + 'q,
{
}

/// Implements [`sealed::DecodeRows`] for a query type by running its own
/// `fetch_*` methods on the statement's rows, so that the rows are decoded the
/// same way as when the query runs on its own.
macro_rules! impl_decode_rows {
    ($( [$($generics:tt)*] $query:ty => $output:ty where [$($bounds:tt)*] )+) => {$(
        impl<'q, $($generics)*> sealed::DecodeRows<'q> for $query
        where
            $($bounds)*
        {
            type Output = $output;

            fn decode_all(self, result: StatementResult) -> ResultFuture<'q, Vec<$output>> {
                Box::pin(async move { self.fetch_all(&mut Replay(Some(result))).await })
            }

            fn decode_one(self, result: StatementResult) -> ResultFuture<'q, $output> {
                Box::pin(async move { self.fetch_one(&mut Replay(Some(result))).await })
            }

            fn decode_optional(self, result: StatementResult) -> ResultFuture<'q, Option<$output>> {
                Box::pin(async move { self.fetch_optional(&mut Replay(Some(result))).await })
            }
        }
    )+};
}

impl_decode_rows! {
    [A] Query<'q, D1, A> => D1Row where [
        A: IntoArguments<'q, D1> + Send + 'q,
    ]
    [O, A] QueryAs<'q, D1, O, A> => O where [
        A: IntoArguments<'q, D1> + Send + 'q,
        O: for<'r> FromRow<'r, D1Row> + Send + Unpin + 'q,
    ]
    [O, A] QueryScalar<'q, D1, O, A> => O where [
        A: IntoArguments<'q, D1> + Send + 'q,
        O: Send + Unpin + 'q,
        (O,): for<'r> FromRow<'r, D1Row> + Send + Unpin,
    ]
    [F, O, A] Map<'q, D1, F, A> => O where [
        F: FnMut(D1Row) -> Result<O, sqlx_core::Error> + Send + 'q,
        O: Send + Unpin + 'q,
        A: IntoArguments<'q, D1> + Send + 'q,
    ]
}

/// An executor returning the stored result of a statement that already ran,
/// whatever the query it's given.
struct Replay(Option<StatementResult>);

impl std::fmt::Debug for Replay {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Replay").finish_non_exhaustive()
    }
}

impl<'c> sqlx_core::executor::Executor<'c> for &'c mut Replay {
    type Database = D1;

    fn fetch_many<'e, 'q: 'e, E>(
        self,
        _query: E,
    ) -> futures_core::stream::BoxStream<
        'e,
        Result<sqlx_core::Either<D1QueryResult, D1Row>, sqlx_core::Error>,
    >
    where
        'c: 'e,
        E: 'q + Execute<'q, D1>,
    {
        let StatementResult { rows, query_result } = self.0.take().unwrap_or_default();

        let items = std::iter::once(Ok(sqlx_core::Either::Left(query_result))).chain(
            rows.into_iter()
                .map(|row| Ok(sqlx_core::Either::Right(row))),
        );

        Box::pin(futures_util::stream::iter(items))
    }

    fn fetch_optional<'e, 'q: 'e, E>(self, _query: E) -> ResultFuture<'e, Option<D1Row>>
    where
        'c: 'e,
        E: 'q + Execute<'q, D1>,
    {
        let row = self
            .0
            .take()
            .and_then(|result| result.rows.into_iter().next());
        Box::pin(async move { Ok(row) })
    }

    fn prepare_with<'e, 'q: 'e>(
        self,
        _sql: &'q str,
        _parameters: &'e [crate::type_info::D1TypeInfo],
    ) -> ResultFuture<'e, crate::statement::D1Statement<'q>>
    where
        'c: 'e,
    {
        Box::pin(async { Err(replay_only("prepare")) })
    }

    fn describe<'e, 'q: 'e>(
        self,
        _sql: &'q str,
    ) -> ResultFuture<'e, sqlx_core::describe::Describe<D1>>
    where
        'c: 'e,
    {
        Box::pin(async { Err(replay_only("describe")) })
    }
}

/// Returns the error of a batch whose statements returned fewer results than
/// there are statements.
fn missing_result() -> sqlx_core::Error {
    sqlx_core::Error::Protocol("D1 returned fewer results than the batch has statements".into())
}

/// Returns the error of an executor method that a statement's stored result
/// can't answer.
fn replay_only(method: &str) -> sqlx_core::Error {
    sqlx_core::Error::Protocol(format!(
        "can't {method} a statement of a batch that already ran"
    ))
}
