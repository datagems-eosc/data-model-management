# Data Stores

The service functions primarily as the data processing workflow horizontal solution for all the related DataGEMS flows. The data it stores include primarily workflow status, executions, etc.

## Relational Database

The primary data store for the service is a PostgreSQL hosted relational database.

The schema of the relational database is the one managed by the Airflow solution.

## Updates

When updating the Airflow service to newer versions, any needed database updates are handled by the migration process of Airflow.
