# Test Suite README

## Notes on Warnings

We ignore the following rdflib deprecation warnings in our pytest configuration:
- `ConjunctiveGraph is deprecated, use Dataset instead:DeprecationWarning`
- `Dataset.default_context is deprecated, use Dataset.default_graph instead:DeprecationWarning`

These warnings are produced by rdflib internals and do not affect test correctness.
Future versions of rdflib may resolve these warnings and make this ignore unnecessary.
