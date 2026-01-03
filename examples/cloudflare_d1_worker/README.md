# SeaORM + Cloudflare D1 Worker example

This example demonstrates using SeaORM's native **Cloudflare D1** driver inside a Cloudflare Worker.

## Endpoints

- `POST /init` – create tables (`users`, `posts`) if they don't exist
- `GET /users` – list users
- `POST /users` – create a user (`{ "name": "Alice" }`)
- `POST /posts` – create a post (`{ "user_id": "<user_id>", "title": "Hello" }`)
- `GET /users/:id/posts` – list posts by user

## Notes

- The Worker must have a D1 binding named `DB` (see `wrangler.toml`).
- `/init` runs in-process migrations via `sea-orm-migration` (D1/SQLite compatible).
- IDs are UUID strings. This allows the example to reload inserted rows reliably.
- D1 can report `0 rows_affected` for successful inserts; the example uses `exec_without_returning` and reloads by ID to avoid `DbErr::RecordNotInserted`.

### About `save()` vs `insert()` on D1

In SeaORM, `ActiveModelTrait::insert` and `ActiveModelTrait::save` may rely on either:

- `RETURNING` support, or
- accurate `rows_affected` / `last_insert_id` metadata.

Cloudflare D1 can return unreliable metadata for some operations. If you encounter
`None of the records are inserted` even though the row exists, prefer the pattern used in this
example: `exec_without_returning` + reload by a deterministic primary key.

If your stack supports SQLite `RETURNING` end-to-end, enabling SeaORM's
`sqlite-use-returning-for-3_35` feature can also help, because inserts/updates will use
`RETURNING` instead of relying on `rows_affected`.
