# SeaORM + Cloudflare D1 Worker example

This example demonstrates using SeaORM's native **Cloudflare D1** driver inside a Cloudflare Worker.

## Endpoints

- `POST /init` – create tables (`users`, `posts`) if they don't exist
- `GET /users` – list users
- `POST /users` – create a user (`{ "name": "Alice" }`)
- `POST /posts` – create a post (`{ "user_id": 1, "title": "Hello" }`)
- `GET /users/:id/posts` – list posts by user

## Notes

- The Worker must have a D1 binding named `DB` (see `wrangler.toml`).
- Table creation uses `Schema::create_table_from_entity` (no `sea-orm-migration` dependency in this example).
