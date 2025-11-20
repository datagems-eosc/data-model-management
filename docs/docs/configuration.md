# Configuration

This service is built using the ASP.NET Core framework, a cross-platform framework for building cloud-based web applications and APIs. ASP.NET Core offers a flexible configuration system that enables developers to manage application settings from a variety of sources, such as JSON files (like appsettings.json), environment variables, command-line arguments, secrets managers, and more. These configuration sources are automatically combined into a single, hierarchical configuration object that the application can use throughout its lifecycle.

One of the key characteristics of ASP.NET Core’s configuration system is its built-in support for environment-specific configurations. Developers can create environment-specific files like appsettings.Development.json, appsettings.Staging.json, or appsettings.Production.json, which override settings from the base configuration file. Additionally, environment variables or command-line arguments can further override any configuration values at runtime. This layered approach makes it easy to manage settings for different deployment environments, enabling a clear separation of concerns and promoting safe and consistent deployment practices.

## Configuration files

In the service we have opted for an approach that splits configuration properties in different configuration files depending on the application behavior they affect.

* accounting.json: Controls how accounting information is generated
* cache.json: Controls which mechanisms will be used for caching
* conversation.json: Controls the behavior of the Conversation feature
* cors.json: Controls CORS policies
* db.json: Controls the database connection
* errors.json: Vocabulary of error codes
* formatting.json: Controls how formatting of common datatypes is performed
* forwarded-headers.json: Controls the discovery of forwarded HTTP headers
* health-check.json: Controls the health check behavior
* idp.claims.json: Controls the extraction of known JWT claims
* idp.client.json: Controls the access of the service to the configured OIDC identity provider
* localization.json: Controls the localization behavior
* log-tracking.json: Controls the log correlation and log enrichment behavior
* logging.json: Controls the logging configuration
* open-api.json: Controls the OpenAPI specification generation
* permissions.json: Controls the authorization grants
* service-airflow.json: Controls the integation with the underpinning Airflow service
* service-cross-dataset-discovery.json: Controlls the integration with the underpinning [Cross Dataset Discovery](https://datagems-eosc.github.io/cross-dataset-discovery/) service
* service-data-management.json: Controlls the integration with the underpinning [Data Management](https://datagems-eosc.github.io/data-model-management/) service
* service-in-data-exploration.json:  Controlls the integration with the underpinning [In Data Exploration](https://datagems-eosc.github.io/in-data-exploration/) service
* user-collection.json: Controls the behavior of the Conversation feature
* vocabulary.json: Vocabularies used for model management

## Environment Overrides

To facilitate configuring the service for different environments and allow reuse of overriding values, the service has a configuration substitution enabled tha tallows defining overriding keys to be used in various configuration values.

For example, in idp.client.json the Idp:Client:Authority configuration option value is set to "%{IdpAuthority}%". If at another configuration file we define a property with a key of "IdpAuthority", the value of that property will substitute the value of the Idp:Client:Authority key.

The practice employed is to define an env.[Environment].json file (eg env.Development.json) file where we place the various keys that need to be substituted. This way, and from a single location, we control the environment specific values.

Additonally, configuration overrides can be applied through environment variables. Only variables starting with the prefix *DG_GW_* are considered by the service configuration stack.

For the ASP.NET environment to be properly bootstrapped, there are two envrionment variables that must be set explicitly:

* ASPNETCORE_ENVIRONMENT: This must define the envrionment in which the service runs. This value will control the "Environment" value in any xxx.[Environment].json configuration file that must be loaded
* ASPNETCORE_URLS: Indicates the IP addresses or host addresses with ports and protocols that the server should listen on for requests

## Secrets

Some of the configuration values contain sensitive data and should be treated differently. These may include secrets, connection strings, etc. It is suggested that, depending on the available infrastructure and tooling, the handling of these values is done separately from other configuration values.
