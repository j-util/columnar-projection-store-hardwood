# Changelog

All notable user-visible changes are documented in this file.

## 1.1.0 - Unreleased

### Changed

- Convenience loading now reads and exactly sums every supplied file's cached
  footer before allocating, so ordered multi-file materialization starts with
  the combined row-count capacity instead of the first file's row-count hint.
- Indexed footer-read failures are exposed as `UncheckedIOException` without
  adding a checked exception to the generated loader API; combined row-count
  overflow is reported as `ArithmeticException`.

## 1.0.0 - 2026-08-12

### Added

- `@HardwoodProjection`, a source-retention annotation for Columnar Projection
  Store schemas.
- A standard annotation processor that discovers generated 1.2.0 store and
  ranged-batch contracts and emits schema-specific Hardwood loaders.
- Direct mappings for Parquet booleans, numeric primitives, strings, and
  binary values, including nullable reference columns.
- Pre-read validation for missing, nested, repeated, nullable primitive, and
  physically incompatible columns.
- Ordered single-file and multi-file materialization with capacity growth from
  the first file's row-count hint, including empty first files.
- Maven Central-ready metadata, release configuration, Maven Wrapper, Java 21
  baseline, Java 21/26 CI, documentation, and end-to-end Parquet tests.
