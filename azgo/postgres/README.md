# postgres

## install
```
go get github.com/lib/pq
```

- Create an Azure Database for PostgreSQL (e.g. [via the Azure Portal](https://docs.microsoft.com/en-ca/azure/postgresql/quickstart-create-server-database-portal)).
- If you did not record it at deployment time, reset the password via the "Reset password" button on the "Overview" blade.
- Copy "PostgreSQL connection URL" from the "Connection strings" blade. It will look similar to: `postgres://username:{your_password}@pg210600.postgres.database.azure.com/postgres?sslmode=require`
- Set an environment variable POSTGRES_URL='postgres://username:{your_password}@pg210600.postgres.database.azure.com/postgres?sslmode=require' in _/AUTH_postgres.sh, and replace `{your_password}` with the correct password.

## citus
check out columnar compression:

<https://www.citusdata.com/blog/2021/03/06/citus-10-columnar-compression-for-postgres/>
