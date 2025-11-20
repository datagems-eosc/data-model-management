# Logging

All DataGEMS services are producing logs in a structured way, usilizing common log formats. The logs are aggregated to the [Logging Service](https://datagems-eosc.github.io/dg-logging-service) where they can be queried and analyzed.

## Log format

The service utilized serilog for structured logging. The respective [Configuration](configuration.md) section describes where this is configured.

The log format utilized by is compact json providing structured presentation of the information presented. This is easily parsed and made available for further processing.

## Correlation Identifier

A key property in enabling troubleshooting in the micro-service DataGEMS architecture is the Correlation Identifier.

In order to serve a user request, a number of services invocations may be chained. It will be useful to be able to track the chain of the request across all involved services. To achive this, we utilize a shared Correlation Id that is generated early in the call stack and propagated across all subsequent invocations.

At the begining of the request stack, we check if there is a correlation id provided for the request in the request headers typically under a header named x-tracking-correlation. If not, we generate one for the request and any downstream calls. We also add it in the logging configuration so that all subsequent log messages include this correlation id.

At the time of invoking another service, we include the correlation id header, along with the correlation id value so that the next service in line will use the same identifier.

The respective [Configuration](configuration.md) section describes where this behavior is configured.

## Troubleshooting Logs

Troubleshooting logs are produced by the service throughout the execution of caller requests. The messages are separated by the log level:

* Trace
* Debug
* Information
* Warning
* Error
* Critical

Log entries may contain the following information (where available):

* Timestamp in UTC (ISO8601)
* Correlation Identifier
* Subject Id
* Client Id
* Message text
* Log Level
* ... additional properties

An prettified example of a log entry is:

```json
[
	{
		"@t": "2025-08-08T11:13:14.8335054Z",
		"@mt": "{\"msg\":\"building\",\"m\":{\"type\":\"Collection\",\"fields\":{\"Fields\":[\"id\",\"code\",\"name\",\"datasetcount\"]},\"dataCount\":5}}",
		"@l": "Debug",
		"@tr": "c3e3abe1e71da568c1dceb387b19a7ff",
		"@sp": "5eee2c1f0d5309b2",
		"SourceContext": "DataGEMS.Gateway.App.Model.Builder.CollectionBuilder",
		"ActionId": "654d5a3b-9980-4684-a70a-4eedaada7b99",
		"ActionName": "DataGEMS.Gateway.Api.Controllers.DatasetController.Query (DataGEMS.Gateway.Api)",
		"RequestId": "0HNEMDADKBH0J:00000001",
		"RequestPath": "/api/dataset/query",
		"ConnectionId": "0HNEMDADKBH0J",
		"ClientId": null,
		"Username": null,
		"UserId": "the subject id",
		"DGCorrelationId": "the correlation id",
		"MachineName": "the machine name",
		"ProcessId": 1,
		"ThreadId": 19,
		"Application": "the application id"
	},
	{
		"@t": "2025-08-08T11:13:14.8357014Z",
		"@mt": "HTTP {RequestMethod} {RequestPath} responded {StatusCode} in {Elapsed:0.0000} ms",
		"@r": [
			"38.1723"
		],
		"@tr": "03cb7b58744f0d0e3ffff2e05ce85ed5",
		"@sp": "cfc6357aff7c7c7c",
		"RequestMethod": "POST",
		"RequestPath": "/api/dataset/query",
		"StatusCode": 200,
		"Elapsed": 38.172305,
		"SourceContext": "Serilog.AspNetCore.RequestLoggingMiddleware",
		"DGCorrelationId": "the correlation id",
		"RequestId": "0HNEMDADKBH0H:00000001",
		"ConnectionId": "0HNEMDADKBH0H",
		"MachineName": "the machine name",
		"ProcessId": 1,
		"ThreadId": 19,
		"Application": "the application id"
	}
]
```

## Accounting Logs

The service generates accounting entries that utilize the same logging mechanism but are differentiated by troubleshooting logs through the "SourceContext" property which is set to "accounting".

These accounting log entries are harvested and processed by the [Accounting Service](https://datagems-eosc.github.io/dg-accounting-service)

A prettified example of an accounting log entry is:
```json
{
	"@t": "2025-08-08T11:13:14.8354027Z",
	"@mt": "{\"m\":{\"timestamp\":\"2025-08-08T11:13:14.8353562Z\",\"serviceId\":\"the service id\",\"action\":\"Query\",\"resource\":\"Dataset\",\"userId\":\"the subject id\",\"value\":\"1\",\"measure\":\"Unit\",\"type\":\"+\"}}",
	"@tr": "03cb7b58744f0d0e3ffff2e05ce85ed5",
	"@sp": "cfc6357aff7c7c7c",
	"SourceContext": "accounting",
	"ActionId": "654d5a3b-9980-4684-a70a-4eedaada7b99",
	"ActionName": "DataGEMS.Gateway.Api.Controllers.DatasetController.Query (DataGEMS.Gateway.Api)",
	"RequestId": "0HNEMDADKBH0H:00000001",
	"RequestPath": "/api/dataset/query",
	"ConnectionId": "0HNEMDADKBH0H",
	"ClientId": null,
	"Username": null,
	"UserId": "the subject id",
	"DGCorrelationId": "the correlation id",
	"MachineName": "the machine name",
	"ProcessId": 1,
	"ThreadId": 19,
	"Application": "the application id"
}

```

The *@mt* property contains the information that is intended to be utilized by the accounting service to track this action:
```json
{
	"m": {
		"timestamp": "2025-08-08T11:13:14.8353562Z",
		"serviceId": "the service id",
		"action": "Query",
		"resource": "Dataset",
		"userId": "the subject id",
		"value": "1",
		"measure": "Unit",
		"type": "+"
	}
}
```
