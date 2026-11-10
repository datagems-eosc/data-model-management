# Configuration

The service reads its configuration from environment variables when it starts. It stops with an error naming what is missing unless the AAI realm and the URLs of MoMa and of the four DataGEMS services it calls are set.

The repository's README lists every variable:

- [the required service URLs](https://github.com/datagems-eosc/data-model-management/blob/main/README.md#required-service-urls);
- [the variables without defaults](https://github.com/datagems-eosc/data-model-management/blob/main/README.md#required-environment-variables-no-defaults), needed only by the endpoints they name, including the PostgreSQL reader account;
- [the optional variables](https://github.com/datagems-eosc/data-model-management/blob/main/README.md#optional-environment-variables), with their defaults.

[LOGGING.md](https://github.com/datagems-eosc/data-model-management/blob/main/LOGGING.md#settings) lists the logging variables.
