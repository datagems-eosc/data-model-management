# Automations

A number of automations are available to facilitate the development, quality assurance, security, deployment, maintenance and onboarding of the service. Here we describe some that are directly, publicly available.

## Documentation

A GitHub Action workflow is available to [generate documentation](https://github.com/datagems-eosc/dg-data-workflow/blob/main/.github/workflows/deploy-docs-on-demand.yml) available in the project in the format presented here. The action is triggered manually and can be executed against the head of the repository. The documentation generated is versioned and the documentation version is expected as input to the workflow.

The documentation is build using the [mkdocs](https://www.mkdocs.org/) library and specifically using the [Material for mkdocs](https://squidfunk.github.io/mkdocs-material/) plugin. A number of additional tools are ustilized, such as [mike](https://github.com/jimporter/mike) to support versioning, and others.

The documentation is generated and tagged with the provided version. It is uploaded to a dedicated documentation branch that is configured to be used as the base branch over which the repository GitHub Page presents its contents.
