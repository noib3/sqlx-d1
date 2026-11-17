#[cfg(feature = "DEBUG")]
pub mod issue_51;
#[cfg(feature = "DEBUG")]
pub mod readme_sample;

use serde::{Deserialize, Serialize};
use uuid::fmt::Hyphenated as HyphenatedUuid;

mod js {
    //! `uuid::Uuid::new_v4()` is not available on Cloudflare Workers,
    //! so we use JavaScript's `crypto.randomUUID` instead.

    use worker::wasm_bindgen;

    #[wasm_bindgen::prelude::wasm_bindgen]
    extern "C" {
        #[wasm_bindgen(js_namespace = crypto)]
        pub fn randomUUID() -> String;
    }
}

#[derive(Serialize, sqlx_d1::FromRow)]
struct User {
    id: i64,
    uuid: Option<HyphenatedUuid>,
    name: String,
    age: Option<i64>,
}

#[derive(Deserialize, Debug)]
struct CreateUserRequest {
    name: String,
    age: Option<u8>,
}

#[worker::event(fetch)]
async fn main(
    req: worker::Request,
    env: worker::Env,
    _ctx: worker::Context,
) -> worker::Result<worker::Response> {
    #[cfg(debug_assertions)]
    console_error_panic_hook::set_once();

    let d1 = env.d1("DB")?;
    let conn = sqlx_d1::D1Connection::new(d1);

    worker::Router::new()
        .get_async("/", |_req, _ctx| {
            let conn = conn.clone();
            async move {
                let users = sqlx_d1::query_as!(
                    User,
                    "
                    SELECT id, uuid, name, age FROM users
                "
                )
                .fetch_all(&conn)
                .await
                .map_err(|e| worker::Error::RustError(e.to_string()))?;

                worker::Response::from_json(&users)
            }
        })
        .post_async("/", |mut req, _ctx| {
            let conn = conn.clone();
            async move {
                let CreateUserRequest { name, age } = req.json().await?;

                let uuid =
                    HyphenatedUuid::from_uuid(uuid::Uuid::parse_str(&js::randomUUID()).unwrap());

                let created_id = sqlx_d1::query_scalar!(
                    "
                    INSERT INTO users (uuid, name, age) VALUES (?, ?, ?)
                    RETURNING id
                ",
                    uuid,
                    name,
                    age
                )
                .fetch_one(&conn)
                .await
                .map_err(|e| worker::Error::RustError(e.to_string()))?;

                worker::Response::from_json(&User {
                    id: created_id,
                    uuid: Some(uuid.into()),
                    name: name.to_string(),
                    age: age.map(|a| a.into()),
                })
                .map(|res| res.with_status(201))
            }
        })
        .post_async("/no_returning", |mut req, _ctx| {
            let conn = conn.clone();
            async move {
                let CreateUserRequest { name, age } = req.json().await?;

                let uuid =
                    HyphenatedUuid::from_uuid(uuid::Uuid::parse_str(&js::randomUUID()).unwrap());

                sqlx_d1::query!(
                    "INSERT INTO users (uuid, name, age) VALUES (?, ?, ?)",
                    uuid,
                    name,
                    age
                )
                .execute(&conn)
                .await
                .map_err(|e| worker::Error::RustError(e.to_string()))?;

                worker::Response::empty().map(|res| res.with_status(204))
            }
        })
        .get_async("/:id", |_req, ctx| {
            let conn = conn.clone();
            async move {
                let id: u32 = ctx
                    .param("id")
                    .ok_or_else(|| worker::Error::RustError("Missing id parameter".to_string()))?
                    .parse()
                    .map_err(|_| worker::Error::RustError("Invalid user ID".to_string()))?;

                let user_record = sqlx_d1::query!(
                    "
                    SELECT uuid, name, age FROM users
                    WHERE id = ?
                ",
                    id
                )
                .fetch_optional(&conn)
                .await
                .map_err(|e| worker::Error::RustError(e.to_string()))?
                .ok_or_else(|| worker::Error::RustError(format!("User(id = {id}) not found")))?;

                let uuid = user_record
                    .uuid
                    .map(|uuid| HyphenatedUuid::from_uuid(uuid::Uuid::parse_str(&uuid).unwrap()));

                let user = User {
                    id: id.into(),
                    uuid,
                    name: user_record.name,
                    age: user_record.age,
                };

                worker::Response::from_json(&user)
            }
        })
        .post_async("/batch", |mut req, _ctx| {
            let conn = conn.clone();
            async move {
                let requests: Vec<CreateUserRequest> = req.json().await?;

                // Either all of the users are created, or none of them are.
                let created_ids = conn
                    .batch(
                        requests
                            .iter()
                            .map(|CreateUserRequest { name, age }| {
                                sqlx_d1::batch::fetch_one(sqlx_d1::query_scalar!(
                                    "INSERT INTO users (name, age) VALUES (?, ?) RETURNING id",
                                    name,
                                    age
                                ))
                            })
                            .collect::<sqlx_d1::batch::Batch<Vec<_>>>(),
                    )
                    .await
                    .map_err(|e| worker::Error::RustError(e.to_string()))?;

                worker::Response::from_json(&created_ids).map(|res| res.with_status(201))
            }
        })
        .post_async("/batch/count", |mut req, _ctx| {
            let conn = conn.clone();
            async move {
                let CreateUserRequest { name, age } = req.json().await?;

                let (created_id, count) = conn
                    .batch((
                        sqlx_d1::batch::fetch_one(sqlx_d1::query_scalar!(
                            "INSERT INTO users (name, age) VALUES (?, ?) RETURNING id",
                            name,
                            age
                        )),
                        sqlx_d1::batch::fetch_one(sqlx_d1::query_scalar!(
                            "SELECT COUNT(*) FROM users"
                        )),
                    ))
                    .await
                    .map_err(|e| worker::Error::RustError(e.to_string()))?;

                worker::Response::from_json(&(created_id, count))
            }
        })
        .get_async("/batch/names", |_req, _ctx| {
            let conn = conn.clone();
            async move {
                let (names, missing) = conn
                    .batch((
                        sqlx_d1::batch::fetch_all(sqlx_d1::query_scalar!(
                            "SELECT name FROM users ORDER BY id"
                        )),
                        sqlx_d1::batch::fetch_optional(sqlx_d1::query_scalar!(
                            "SELECT name FROM users WHERE id = 0"
                        )),
                    ))
                    .await
                    .map_err(|e| worker::Error::RustError(e.to_string()))?;

                worker::Response::from_json(&(names, missing))
            }
        })
        .post_async("/batch/rollback", |_req, _ctx| {
            let conn = conn.clone();
            async move {
                let count =
                    || sqlx_d1::query_scalar!("SELECT COUNT(*) FROM users").fetch_one(&conn);

                let before = count()
                    .await
                    .map_err(|e| worker::Error::RustError(e.to_string()))?;

                // The second insert breaks the `NOT NULL` constraint on `name`, so
                // the first one is rolled back too.
                let error = conn
                    .batch((
                        sqlx_d1::batch::execute(sqlx_d1::query!(
                            "INSERT INTO users (name) VALUES ('rolled back')"
                        )),
                        sqlx_d1::batch::execute(sqlx_d1::query(
                            "INSERT INTO users (name) VALUES (NULL)",
                        )),
                    ))
                    .await
                    .err()
                    .map(|e| e.to_string());

                let after = count()
                    .await
                    .map_err(|e| worker::Error::RustError(e.to_string()))?;

                worker::Response::from_json(&(before, after, error))
            }
        })
        .run(req, env)
        .await
}
