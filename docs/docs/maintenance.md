# Maintenance

The service is part of the DataGEMS platform offered through an existing deployment, following the DataGEMS release and deployment procedures over a managed infrasrtucture along with the maintenance activities that are scheduled within the platform. The purpose of this section is not to detail the maintenance activities put in place by the DataGEMS team.

## Healthchecks

The service [observability documentation](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/logging-monitoring/check-health.html) describes healthcheck endpoints that can be used to track the status of the service.

An example of a healthcheck response that returns 200 OK for healthy state is:
```json
{
  "metadatabase":{
    "status":"healthy"
  },
  "scheduler":{
    "status":"healthy",
    "latest_scheduler_heartbeat":"2018-12-26 17:15:11+00:00"
  },
  "triggerer":{
    "status":"healthy",
    "latest_triggerer_heartbeat":"2018-12-26 17:16:12+00:00"
  },
  "dag_processor":{
    "status":"healthy",
    "latest_dag_processor_heartbeat":"2018-12-26 17:16:12+00:00"
  }
}
```

## Verions & Updates

The service follows the versioning and update scheme that Airflow supports.

## Backups

All state persisted by the service is maintained in the relational database as described in the respective [datastores](datastore.md) section.

To keep backups of the state, the respective utilities must be scheduled to run in a consistent manner.

## Troubleshooting

Troubleshooting is primarily done through the logging mechanisms that are available within Airflow. These offer log reports on individual tasks and flows as they are executed by the Airflow engine.
