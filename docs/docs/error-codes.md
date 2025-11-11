# Status & Error Codes

The HTTP Api exposed by the service exposes restful endpoints to offer its functionality. These endpoints are documented using the [OpenAPI specification](http://openapis.org/) and available for integrators to consult in the respective [API section](openapi.md).

In this section, we describe further the HTTP Status Codes that the service may expose and possible Error Codes that can be expected.

## Status Codes

Commonly used HTTP Response codes are in the service. It makes proper usage of these HTTP Status Codes and tries not to abuse their semantics. The following subset is used and documented in the respective [OpenAPI document](openapi.md):

* 200 OK - request has succeeded
* 400 Bad Request - request not processed due to a request/client error
* 401 Unauthorized - request was not successful because it lacks valid authentication credentials
* 403 Forbidden - understood the request but refused to process it. Authenticating or re-authenticating makes no difference. The request failure is tied to application logic, such as insufficient permissions to a resource or action
* 404 Not Found - cannot find the requested resource
* 424 Failed Dependency - method could not be performed on the resource because the requested action depended on another action, and that action failed
* 500 Internal Server Error - encountered an unexpected condition that prevented the service from fulfilling the request

## Error Codes

For unsucessful requests (other than 200 OK), there may be additional information that can and are provided to the caller. They should be considered as refinements of the error category that is defined through the respective HTTP Status Code of the response. The additional information is provided in both a system friendly error code as well as a user friendly message.

The service exposes the following error codes, depending on the HTTP Status Code that will be returned:

* System Error
    * Code: 100
    * Error: an unexpected system error occured
* Forbidden
    * Code": 101
    * Error: insufficient rights
* Model Validation
    * Code: 102
    * Error: validation error structured description
* Unsupported Action
    * Code: 103
    * Error: request for unsupported action
* Underpinning Service
    * Code: 104
    * Error: error communicating with underpinning service
* Token Exchange
    * Code: 105
    * Error: error exchanging tokens for underpinning service
* User Sync
    * Code: 106
    * Error: authorized user out of sync with internal registry
* Concurrent Update Conflict
    * Code: 107
    * Error: there is an etag conflict for the item modifed with Id = X of Type = Y. please reload to get the latest changes
* Immutable Item
    * Code: 108
    * Error: you are trying to modify an immutable item or property

## Bad requests

Specifically for the case of responses with status code 400 (Bad Request), with an error code 102 (Model Validation), the response message may include additional structured information on the kind of error that was identified.

For example, if the caller is requesting a paged list of results without providing the ordering to be used and the service does not support default ordering, the response will be a 400 Bad Request with a payload as bellow

```json
{
    "code": 102,
    "error": "Validation Error",
    "message": [
        {
            "Key": "Page",
            "Value": [
                "paging not supported without ordering"
            ]
        }
    ]
}
```

The "Key" binds directly to the request property that was problematic.

In case the request had an array of objects and one of the properties within the array objects was failty, the key would indicate the specific failty object and property. So, if we try to update a User Collection providing also the links to the Datasets that is should contain but we do not provide the dataset id for the 4th item in the list (index 3), the response will look like the following:

```json
{
    "code": 102,
    "error": "Validation Error",
    "message": [
        {
            "Key": "UserDatasetCollections[3].DatasetId",
            "Value": [
                "DatasetId is required"
            ]
        }
    ]
}
```

## Failed Dependency

When this status code is used, the service, in the process of serving the request, calls other services. One of its underpinning services did not operate as expected to complete the processing of the request sucessfuly. The response status code would be a 424 Failed Dependency with the needed code and error message. But it can also contain additional information about the underpinning service reported error. This way, the response will contain all the information that the service can provide, without needed to redefine all the possible error conditions of its underpinning services. Additionally, this gives the possibility to track the error stack across multiple services if needed. An example of such a case might be:

```json
{
    "code": 104,
    "error": "error communicating with underpinning service",
    "message": {
        "statusCode": 500,
        "source": "the service name",
        "correlationId": "log correlation identifier"
    }
}
```

The intend of propagating additional payload from the underpining service response in the payload is to assist in identifying issues that can be managed in the context of the request, not to provide general troubleshooting capabilities. For the later, the correlationId is included so that if reported additonal information can be retrieved from the logsm as covered in the relevant [Logging](logging.md) section. But in case the underpinning service response code is a 400 Bad Request, that means that this is something that the caller will need to be notified about as it may be something that can be fixed in the context of the request. So, in such cases, another example could be:

```json
{
    "code": 104,
    "error": "error communicating with underpinning service",
    "message": {
        "statusCode": 400,
        "source": "the service name",
        "correlationId": "log correlation identifier",
        "payload": {
          "code": 102,
          "error": "Validation Error",
          "message": [
              {
                  "Key": "Page",
                  "Value": [
                      "paging not supported without ordering"
                  ]
              }
          ]
        }
    }
}
```
