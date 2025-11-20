# Data Stores

The service functions primarily as a gateway between the user interface, or other external integration points, and the core DataGEMS services. While it does not serve as the main repository for datasets or models related to most user requests, it plays a critical role in facilitating access to core platform features. As such, it must manage certain operational data necessary to support its responsibilities and ensure smooth interaction with the broader system.

## Relational Database

The primary data store for the service is a PostgreSQL hosted relational database.

The schema of the relational database can be found in the service repository under the [db](https://github.com/datagems-eosc/dg-app-api/tree/main/db) folder.

## Change scripts

The evolution of the database is managed through ordered database change scripts. The scripts incrimentaly update the database schema to target the desired version. The change scripts are numbered and versioned in the service repository under the [db](https://github.com/datagems-eosc/dg-app-api/tree/main/db) folder.

Intermediate "XX.XX.XXX-Seed.Scema.sql" scripts can be used to initialize the database in the sepecific version so that previous change scripts do not need to be applied.
