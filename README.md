# API_MIGRADOR_TABLAS

This project is a minimal API for migrating tables from a SQL Server database (MERENDON) into PostgreSQL (`siad` schema).

## New endpoint
- POST `/api/migrate/cln_clientes` — migrate `CLN_CLIENTES` from SQL Server to `siad.cln_clientes` in Postgres.
  - Query params:
    - `truncate` (bool) — if true, runs `TRUNCATE TABLE siad.cln_clientes;` before loading (default: false)
    - `createIfMissing` (bool) — if true, creates the destination table using a best-effort mapping from SQL Server types (default: false)
    - `skipOnPk` (bool) — if true (default) the migration will skip rows that would conflict with the destination primary key (uses a temp table + `ON CONFLICT (...) DO NOTHING`). If the destination has no primary key and `skipOnPk=true` the operation will fail with an explanatory error.

- POST `/api/migrate/tmp_seleccion_cln` — migrate `TMP_SELECCION_CLN` from SQL Server to `siad.tmp_seleccion_cln` in Postgres.
  - Query params:
    - `truncate` (bool) — if true, runs `TRUNCATE TABLE siad.tmp_seleccion_cln;` before loading (default: false)
    - `createIfMissing` (bool) — if true, creates the destination table using a best-effort mapping from SQL Server types (default: false)
    - `skipOnPk` (bool) — if true (default) the migration will skip rows that would conflict with the destination primary key (uses a temp table + `ON CONFLICT (...) DO NOTHING`). If the destination has no primary key and `skipOnPk=true` the operation will fail with an explanatory error.

- POST `/api/migrate/inv_existencias` — migrate `INV_EXISTENCIAS` from SQL Server to `siad.inv_existencias` in Postgres.
  - Query params:
    - `truncate` (bool) — if true, runs `TRUNCATE TABLE siad.inv_existencias;` before loading (default: false)
    - `createIfMissing` (bool) — if true, creates the destination table using a best-effort mapping from SQL Server types (default: false)
    - `skipOnPk` (bool) — if true (default) the migration will skip rows that would conflict with the destination primary key (uses a temp table + `ON CONFLICT (...) DO NOTHING`). If the destination has no primary key and `skipOnPk=true` the operation will fail with an explanatory error.

Example:

    POST http://localhost:5000/api/migrate/cln_clientes?truncate=true&createIfMissing=true&skipOnPk=true
    POST http://localhost:5000/api/migrate/tmp_seleccion_cln?truncate=true&createIfMissing=true&skipOnPk=true

Notes:
- The service checks that the source table exists in SQL Server and the destination in Postgres; if the destination is missing and `createIfMissing=true` it will create a table using a basic type mapping.
- Large tables are imported using PostgreSQL binary COPY into a temporary table, then inserted into the destination with conflict handling (if enabled).
- Be careful with `truncate` because it will remove all data from the destination table.

Notes:
- The service checks that the source table exists in SQL Server and the destination in Postgres; if the destination is missing and `createIfMissing=true` it will create a table using a basic type mapping.
- Large tables are imported using PostgreSQL binary COPY for performance.
- Be careful with `truncate` because it will remove all data from the destination table.
