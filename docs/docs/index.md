# DataGEMS Data Model Management API

This site documents the Data Model Management API, a service of the [DataGEMS](https://datagems.eu/) platform. The [platform documentation](https://datagems-eosc.github.io/) covers the platform as a whole.

The API registers, loads and updates datasets described in Croissant, keeping their metadata in MoMa and their files on the platform's storage. It runs SQL queries over a dataset's files and tables, and it forwards requests to the DataGEMS services that discover, explore and recommend datasets. Requests and results take the form of Analytical Patterns.

To start, read the [Architecture](architecture.md) and the [API Overview](api-overview.md), then the [OpenAPI Reference](openapi.md). [Status & Error Codes](error-codes.md) explains the error responses.

The code is in the [GitHub repository](https://github.com/datagems-eosc/data-model-management); report a problem in its [issues](https://github.com/datagems-eosc/data-model-management/issues). For anything else, contact the [DataGEMS Help Desk](https://datagems.eu/contact-us/).
