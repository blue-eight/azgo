# postgres

- Create an Azure Database for PostgreSQL (e.g. [via the Azure Portal](https://docs.microsoft.com/en-ca/azure/postgresql/quickstart-create-server-database-portal)).
- If you did not record it at deploymeny time, reset the password via the "Reset password" button on the "Overview" blade.
- Copy "PostgreSQL connection URL" from the "Connection strings" blade. It will look similar to: `postgres://username:{your_password}@pg210600.postgres.database.azure.com/postgres?sslmode=require`
- Set an environment variable POSTGRES_URL='postgres://username:{your_password}@pg210600.postgres.database.azure.com/postgres?sslmode=require' in _/AUTH_postgres.sh, and replace `{your_password}` with the correct password.
