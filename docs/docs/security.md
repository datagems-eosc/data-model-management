# Security

Key aspects of the Security checklist and practices that DataGEMS services must pass have been defined in the processes and documents governing the platform development and quality assurance. In this section we present a selected subset of these that are directly, publicly available and affect the usage and configuration of the service.

## Authentication

All endpoints exposed by this service require authentication using Bearer tokens in the form of JWTs (JSON Web Tokens). Clients must include a valid token in the Authorization header of each HTTP request, using the following format:

```
Authorization: Bearer <token>
```

The service only accepts JWTs that are issued by a trusted identity provider, the [DataGEMS AAI service](https://github.com/datagems-eosc/dg-aai). This issuer is responsible for authenticating users and issuing tokens with claims that the service can validate. One of the critical claims in the token is the *aud* (audience) claim. The value of this claim must include the identifier of this service, ensuring that the token was intended to be used with it.

When a token is received, the service performs a series of validation steps before granting access to any endpoint. These steps typically include verifying the token’s signature using the public keys published by the trusted issuer, checking the issuer *iss* claim to confirm it matches the expected DataGEMS AAI issuer, validating the audience *aud* claim to ensure the token was meant for this service, and checking the token expiration *exp* to confirm the token is still valid. Only if all these checks pass will the request be authenticated and passed for further processing.

The location of the configuration governing the specific behavior is described in teh relevant [Configuration](configuration.md) section.

## Authorization

When an authenticated call reaches the service, the caller may be authorized to perform an action or not. This will have to be authorized based on the grants that are present as roles in the access token presented as well as the context in which they want to perform the operation.

Within the service, all data access operations as well as individual actions pass authorization checks. The permissions that are checked along with the policies attached to each one is managed in a configuration file that the respective [Configuration](configuration.md) section describes.

Possible authorization policies include:

* **Context-less assignment**: such as an administrator that can perform action X on entity Y, regardless of the kind of affiliation they have with the entity
* **Context**: An affiliation of the calling user with the entity over which the action is to be performed. The context policy it tied to an anchor entity that is treated as the affiliation bearer. In the context of DataGEMS, this entity is the Dataset
* **Owner**: Specific kind of affiliation between the calling user and the entity over which the action is to be performed, indicating ownership of the entity. An example is a User Collection that is owned by a specific user
* **Claim**: a policy similar to the Context-less permission assignment. Instead of checking specificaly the "role" claim type, this policy will check ad-hoc claims that have specific values to match the policy
* **Client**: a policy similar to the Context-less permission assignment. Instead of checking specificaly the "role" claim type, this policy will check the caller client id to have a specific value to match the policy
* **Authenticated**: Authenticated user, regardless of any other characteristic vcan be granted or not granted the specific permission
* **Anonymous**: Anonymous users be be granted or not granted the specific permission

For context based authorization policies with a Dataset affiliation bearer, the evaluation of the kind of operations that the user can perfomr is done by interpreting the respective JWT "datasets" claim. The context is provided through the interpretation of the context grant model detailed in the [DataGEMS AAI](https://datagems-eosc.github.io/dg-aai/).

For each of affiliation bearing datasets, the following actions (verbs) are interpreted to grant needed access:
* browse - Grants permission to browse the dataset in a listing view and see dataset metadata
* delete - Grants permission to delete the dataset
* download - Grants permission to download the dataset
* edit - Grants permission to edit dataset data and metadata
* search - Grants permission to perform a search using the data and metadata of the dataset. This does not refer to listing / browsing operations but advanced search capabilities offered by DataGEMS

## Token exchange

In the microservice architecture of DataGEMS and given the role of the Gateway Api in the DataGEMS archtiecture, it is expected that the service will need to invoke other services, either outside or within the request flow of an external invocation. The service  may need to perform some out of band operation or a scheduled maintenance / system task. Or, within the context of an incoming communication, invoke a third service as part of fulfilling its operation. We separate these two cases to distinguish the authentication mechanisms that are used:

* **Service to Service with Client Credentials**: The service needs to acquire an access token that it can present to the service it is invoking. It will initiate a client credential flow, presenting it's client id and secret, along with the scope of the access token which will indicate the audience / reciever of the generated credential. The genereted access token can then be used in the HTTP request as a Bearer token in the Authorization header.

* **Service to Service with Exchanged Credentials**: The service, in the context of serving a user request needs to invoke another service. It must forward the credentials it was invoked with to continue the process flow under the scope of the original caller. It will initiate a token exchange flow, presenting it's client id and secret so that the AAI service authorizes the client to exhcnage the requested token, along with the initial access token that needs to be exchanged and the desired scope of the new access token which will indicate the audience / reciever of the generated, exchanged credential. The genereted access token can then be used in the HTTP request as a Bearer token in the Authorization header.

These two flows are supported by the [DataGEMS AAI service](https://github.com/datagems-eosc/dg-aai) and the needed configuration is located as the respective [Configuration](configuration.md) section describes.

## Secrets

Secrets are a special kind of configuration that requires special handling. This is described in the respective [Configuration](configuration.md) section.
