//! Integration test for the libsql driver — exercises real connect, DDL,
//! CRUD, and transactions against an in-memory libsql database.

use sea_orm::entity::prelude::*;
use sea_orm::{
    ColumnTrait, Database, DatabaseConnection, EntityTrait, QueryFilter, QueryOrder, Set,
    TransactionTrait,
};

#[derive(Clone, Debug, PartialEq, Eq, sea_orm::DeriveEntityModel)]
#[sea_orm(table_name = "libsql_test_items")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: i64,
    pub name: String,
    pub qty: i32,
}

#[derive(Copy, Clone, Debug, EnumIter, sea_orm::DeriveRelation)]
pub enum Relation {}

impl sea_orm::ActiveModelBehavior for ActiveModel {}

async fn setup(db: &DatabaseConnection) {
    use sea_orm::Schema;
    let backend = db.get_database_backend();
    let schema = Schema::new(backend);
    let mut stmt = schema.create_table_from_entity(Entity);
    stmt.if_not_exists();
    let db_stmt = backend.build(&stmt);
    db.execute_unprepared(&db_stmt.sql).await.unwrap();
}

#[tokio::test]
async fn libsql_crud_and_transaction() {
    let db = Database::connect("libsql:///").await
        .expect("connect to in-memory libsql");
    setup(&db).await;

    // Insert
    let _cake = ActiveModel {
        id: Set(1),
        name: Set("cheesecake".to_owned()),
        qty: Set(3),
    }
    .insert(&db)
    .await
    .expect("insert");

    let _b = ActiveModel {
        id: Set(2),
        name: Set("chocolate".to_owned()),
        qty: Set(5),
    }
    .insert(&db)
    .await
    .expect("insert");

    // Query all ordered by id
    let all = Entity::find()
        .order_by_asc(Column::Id)
        .all(&db)
        .await
        .expect("find all");
    assert_eq!(all.len(), 2);
    assert_eq!(all[0].name, "cheesecake");
    assert_eq!(all[1].name, "chocolate");

    // Filter + update
    let cheesecake = Entity::find()
        .filter(Column::Name.eq("cheesecake"))
        .one(&db)
        .await
        .expect("find one")
        .expect("exists");
    let mut am: ActiveModel = cheesecake.into();
    am.qty = Set(4);
    let updated = am.update(&db).await.expect("update");
    assert_eq!(updated.qty, 4);

    // Transaction: commit path
    db.transaction::<_, _, sea_orm::DbErr>(|txn| {
        Box::pin(async move {
            let inserted = ActiveModel {
                id: Set(3),
                name: Set("strawberry".to_owned()),
                qty: Set(1),
            }
            .insert(txn)
            .await?;
            assert_eq!(inserted.qty, 1);
            Ok(())
        })
    })
    .await
    .expect("transaction commit");

    let strawberry = Entity::find()
        .filter(Column::Name.eq("strawberry"))
        .one(&db)
        .await
        .expect("find")
        .expect("exists");
    assert_eq!(strawberry.qty, 1);

    // Transaction: rollback path — insert then error, ensure rolled back
    let rollback_res = db
        .transaction::<_, _, sea_orm::DbErr>(|txn| {
            Box::pin(async move {
                ActiveModel {
                    id: Set(4),
                    name: Set("lemon".to_owned()),
                    qty: Set(2),
                }
                .insert(txn)
                .await?;
                Err::<(), _>(sea_orm::DbErr::Custom("rollback me".to_owned()))
            })
        })
        .await;
    assert!(rollback_res.is_err());

    let lemon = Entity::find()
        .filter(Column::Name.eq("lemon"))
        .one(&db)
        .await
        .expect("find");
    assert!(lemon.is_none(), "rolled-back insert must not persist");

    // Delete
    let deleted = Entity::delete_many()
        .filter(Column::Name.eq("cheesecake"))
        .exec(&db)
        .await
        .expect("delete");
    assert_eq!(deleted.rows_affected, 1);
}