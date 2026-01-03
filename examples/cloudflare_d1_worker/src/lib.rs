use sea_orm::{
    ColumnTrait, Database, DatabaseConnection, EntityTrait, QueryFilter, Set,
};
use sea_orm_migration::MigratorTrait;
use serde::Deserialize;
use uuid::Uuid;
use worker::*;

mod entity;
mod migration;

const DB_BINDING: &str = "DB";

#[event(fetch)]
pub async fn fetch(req: Request, env: Env, _ctx: Context) -> Result<Response> {
    console_error_panic_hook::set_once();

    Router::new()
        .get_async("/", |_req, _ctx| async move {
            Response::ok("SeaORM + Cloudflare D1 example. Try /init, /users, /posts")
        })
        .post_async("/init", |_req, ctx| async move {
            let db = connect_db(&ctx).await?;
            migration::Migrator::up(&db, None)
                .await
                .map_err(worker_err)?;
            Response::ok("ok")
        })
        .get_async("/users", |_req, ctx| async move {
            let db = connect_db(&ctx).await?;
            let users = entity::user::Entity::find()
                .all(&db)
                .await
                .map_err(worker_err)?;
            Response::from_json(&users)
        })
        .post_async("/users", |mut req, ctx| async move {
            let db = connect_db(&ctx).await?;

            #[derive(Deserialize)]
            struct NewUser {
                name: String,
            }

            let payload: NewUser = req.json().await?;

            // D1 can report 0 rows_affected on INSERT even when the row is created.
            // Avoid relying on insert metadata by inserting without returning and reloading by ID.
            let user_id = Uuid::new_v4().to_string();
            let model = entity::user::ActiveModel {
                id: Set(user_id.clone()),
                name: Set(payload.name),
                ..Default::default()
            };

            entity::user::Entity::insert(model)
                .exec_without_returning(&db)
                .await
                .map_err(worker_err)?;

            let user = entity::user::Entity::find_by_id(user_id)
                .one(&db)
                .await
                .map_err(worker_err)?
                .ok_or_else(|| worker::Error::RustError("Inserted user could not be reloaded".into()))?;

            Response::from_json(&user)
        })
        .get_async("/users/:id/posts", |_req, ctx| async move {
            let db = connect_db(&ctx).await?;
            let user_id: String = ctx
                .param("id")
                .ok_or_else(|| worker::Error::RustError("missing :id".into()))?
                .to_string();

            let posts = entity::post::Entity::find()
                .filter(entity::post::Column::UserId.eq(user_id))
                .all(&db)
                .await
                .map_err(worker_err)?;

            Response::from_json(&posts)
        })
        .post_async("/posts", |mut req, ctx| async move {
            let db = connect_db(&ctx).await?;

            #[derive(Deserialize)]
            struct NewPost {
                user_id: String,
                title: String,
            }

            let payload: NewPost = req.json().await?;

            // D1 insert metadata can be unreliable; insert without returning and reload by ID.
            let post_id = Uuid::new_v4().to_string();
            let model = entity::post::ActiveModel {
                id: Set(post_id.clone()),
                user_id: Set(payload.user_id),
                title: Set(payload.title),
                ..Default::default()
            };

            entity::post::Entity::insert(model)
                .exec_without_returning(&db)
                .await
                .map_err(worker_err)?;

            let post = entity::post::Entity::find_by_id(post_id)
                .one(&db)
                .await
                .map_err(worker_err)?
                .ok_or_else(|| worker::Error::RustError("Inserted post could not be reloaded".into()))?;

            Response::from_json(&post)
        })
        .run(req, env)
        .await
}

async fn connect_db(ctx: &RouteContext<()>) -> Result<DatabaseConnection> {
    let d1 = ctx.d1(DB_BINDING)?;
    let db = Database::connect_d1(d1)
        .await
        .map_err(worker_err)?;
    Ok(db)
}

fn worker_err<E: core::fmt::Display>(e: E) -> worker::Error {
    worker::Error::RustError(e.to_string())
}
