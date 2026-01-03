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
