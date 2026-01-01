use sea_orm::{
    ActiveModelTrait, ColumnTrait, ConnectionTrait, Database, DatabaseConnection, EntityTrait,
    QueryFilter, Schema, Set,
};
use serde::Deserialize;
use worker::*;

mod entity;

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
            init_schema(&db).await?;
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

            let user = entity::user::ActiveModel {
                name: Set(payload.name),
                ..Default::default()
            }
            .insert(&db)
            .await
            .map_err(worker_err)?;

            Response::from_json(&user)
        })
        .get_async("/users/:id/posts", |_req, ctx| async move {
            let db = connect_db(&ctx).await?;
            let user_id: i32 = ctx
                .param("id")
                .ok_or_else(|| worker::Error::RustError("missing :id".into()))?
                .parse()
                .map_err(|e| worker::Error::RustError(format!("invalid :id: {e}")))?;

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
                user_id: i32,
                title: String,
            }

            let payload: NewPost = req.json().await?;

            let post = entity::post::ActiveModel {
                user_id: Set(payload.user_id),
                title: Set(payload.title),
                ..Default::default()
            }
            .insert(&db)
            .await
            .map_err(worker_err)?;

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

async fn init_schema(db: &DatabaseConnection) -> Result<()> {
    let schema = Schema::new(db.get_database_backend());

    // Create in dependency order: users -> posts.
    let mut stmt = schema.create_table_from_entity(entity::user::Entity);
    stmt.if_not_exists();
    db.execute(&stmt).await.map_err(worker_err)?;

    let mut stmt = schema.create_table_from_entity(entity::post::Entity);
    stmt.if_not_exists();
    db.execute(&stmt).await.map_err(worker_err)?;

    Ok(())
}

fn worker_err<E: core::fmt::Display>(e: E) -> worker::Error {
    worker::Error::RustError(e.to_string())
}
