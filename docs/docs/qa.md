# Quality Assurance

Key aspects of the Quality Assurance checklist that DataGEMS services must pass have been defined in the processes and documents governing the platform development and quality assurance. In this section we present a selected subset of these that are directly, publicly available.

## Code Analysis

Static code analysis is the process of examining source code without executing it, to identify potential errors, vulnerabilities, or deviations from coding standards. It is typically performed using tools that analyze the code's structure, syntax, and logic to detect issues such as bugs, security flaws, or maintainability problems early in the development cycle. This helps improve code quality, reduce technical debt, and ensure compliance with best practices before the software is run or deployed.

Static code analysis is a process that has been tied with the development and release lifecycle through the configured GitHub Actions workflow that performs security, quality and maintenability of the code base. The workflow is described in the relevant [Automations](automations.md) section.

## Code Metrics

Code metrics are quantitative measurements used to assess various aspects of source code quality and complexity. They help developers understand how maintainable, efficient, and error-prone a codebase might be. Common code metrics include lines of code (LOC), cyclomatic complexity, maintenability index, and coupling levels. By analyzing these metrics, teams can identify potential issues, enforce coding standards, and improve overall software quality throughout the development lifecycle.

The service has configured an automated GitHub Actions workflow, as described in the relevant [Automations](automations.md) section to calculate such metrics.

## Vulnerability checks

Vulnerability checks are processes used to identify known security weaknesses in software, libraries, or dependencies. These checks typically scan the codebase, configuration files, or external packages against databases of publicly disclosed vulnerabilities. By detecting issues such as outdated libraries, insecure functions, or misconfigurations, vulnerability checks help developers address security risks early and maintain a secure software environment.

The service has configured an automated GitHub Actions workflow, as described in the relevant [Automations](automations.md) section to perform such checks on the versioned artefacts.

## Testing

As the Gateway API mainly provides proxying, aggregation and transformation logic over the underpinning DataGEMS services, an API testing approach has been choosen to perform smoke testing on existing installations of the API, be it in local development environemnt or deployed instances. The testing approach allows the usage of the same testing scenarios while in development as well as in deployed installations. It reuses the internal integration mechanisms provided to facilitate development and communication by reusing Postman request collections.

The testing collections can be found in the service [code repository](https://github.com/datagems-eosc/dg-app-api) in the [tests folder](https://github.com/datagems-eosc/dg-app-api/tree/main/tests).

The test collections can be executed locally using the [Postman](https://www.postman.com/) application, or by command line using the [Newman CLI](https://github.com/postmanlabs/newman). To run the tests locally, one can use the following command using docker with a terminal inside the tests folder. It can also be run without docker after installing the newman library locally.

```console
docker run --rm -v "${PWD}:/etc/newman" postman/newman:6-ubuntu run DataGEMS-GatewayApi-Tests.postman_collection.json --env-var "baseUrl=<...>" --env-var "..." --reporters cli
```

A test user must be used in order to login and permorm the authenticated operations.

The testing collection can also be executed through the CI pipelines as described in the relevant [Automations](automations.md) section.

The output of the test run presents a summary of the requests performed and any errors that may have been observed.

```
┌───────────────────────┬────────────────┬───────────────┐
│                       │       executed │        failed │
├───────────────────────┼────────────────┼───────────────┤
│            iterations │              1 │             0 │
├───────────────────────┼────────────────┼───────────────┤
│              requests │             16 │             0 │
├───────────────────────┼────────────────┼───────────────┤
│          test-scripts │             16 │             0 │
├───────────────────────┼────────────────┼───────────────┤
│    prerequest-scripts │              6 │             0 │
├───────────────────────┼────────────────┼───────────────┤
│            assertions │             16 │             0 │
├───────────────────────┴────────────────┴───────────────┤
│ total run duration: 3.3s                               │
├────────────────────────────────────────────────────────┤
│ total data received: 29.96kB (approx)                  │
├────────────────────────────────────────────────────────┤
│ avg resp time:193ms [min:103ms, max:358ms, s.d.:100ms] │
└────────────────────────────────────────────────────────┘
```
