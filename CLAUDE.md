# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview
Apache Iceberg is a high-performance table format for huge analytic datasets. This is the core Java implementation that serves as the reference for other language implementations.

## Essential Development Commands

### Build Commands
```bash
# Full build with tests
./gradlew build

# Build without tests (faster)
./gradlew build -x test -x integrationTest

# Run unit tests only
./gradlew test

# Run integration tests (requires Docker)
./gradlew integrationTest

# Run all checks (tests + code quality)
./gradlew check

# Run tests for a specific module
./gradlew :iceberg-core:test
./gradlew :iceberg-spark:iceberg-spark-3.5_2.12:test
```

### Code Quality
```bash
# Fix code formatting (ALWAYS run before committing)
./gradlew spotlessApply

# Fix formatting for all engine versions
./gradlew spotlessApply -DallModules

# Check code style without fixing
./gradlew spotlessCheck
```

### Documentation
```bash
# Generate Javadocs
./gradlew aggregateJavadoc
```

## Architecture Overview

### Core Library Structure
The project uses a modular architecture with clear separation of concerns:

1. **API Layer** (`iceberg-api`): Public interfaces defining the Iceberg table format
2. **Core Implementation** (`iceberg-core`): Reference implementation with Avro support
3. **Format Support**: Optional modules for Parquet (`iceberg-parquet`), ORC (`iceberg-orc`), Arrow (`iceberg-arrow`)
4. **Catalog Implementations**: Various catalog backends (Hive Metastore, Nessie, REST, etc.)
5. **Engine Integrations**: Support for Spark, Flink, Hive/MapReduce with version-specific modules

### Multi-Version Engine Support
The build system dynamically creates modules for different engine versions:
- **Spark**: Versions 3.4, 3.5, 4.0 (modules like `iceberg-spark-3.5_2.12`)
- **Flink**: Versions 1.19, 1.20, 2.0 (modules like `iceberg-flink-1.20`)
- **Scala**: Versions 2.12, 2.13 for Spark/Flink compatibility

To build specific versions:
```bash
./gradlew build -DsparkVersions=3.5,4.0
./gradlew build -DflinkVersions=1.20
```

### Key Design Patterns
1. **Table Format Abstraction**: Clean separation between format specification and engine implementations
2. **Catalog Plugin System**: Extensible catalog interface supporting multiple metadata stores
3. **File Format Abstraction**: Support for multiple file formats (Parquet, ORC, Avro) through common interfaces
4. **Snapshot-Based Architecture**: Immutable snapshots for ACID guarantees and time travel

## Development Guidelines

### Java Version Requirements
- **Development**: Java 11, 17, or 21
- **Release Builds**: Always use Java 11
- **Language Features**: Can use Java 11+ features

### Testing Requirements
- **Docker Required**: Integration tests use Testcontainers
- **Test Coverage**: All changes must include appropriate tests
- **Test Organization**: Unit tests in `src/test/java`, integration tests in `src/integrationTest/java`

### Code Style
- Uses Google Java Format (automatically applied via Spotless)
- Apache license headers required on all files
- Import optimization enforced

### API Compatibility
- Core modules maintain backward compatibility
- Use `@Deprecated` with migration path before removal
- API changes require discussion in pull requests

## Common Development Tasks

### Adding a New Feature
1. Create feature branch from `master`
2. Implement changes with tests
3. Run `./gradlew spotlessApply` to fix formatting
4. Run `./gradlew check` to verify all tests pass
5. Update documentation if needed

### Debugging Test Failures
```bash
# Run specific test class
./gradlew :iceberg-core:test --tests TestClassName

# Run with more output
./gradlew test --info

# Check test logs
cat build/testlogs/*.log
```

### Working with Engine-Specific Code
When modifying Spark or Flink integrations:
1. Changes often need to be made across multiple version modules
2. Use the base module (e.g., `iceberg-spark`) for shared code
3. Version-specific code goes in version modules (e.g., `iceberg-spark-3.5_2.12`)

## Project-Specific Notes

### Catalog Implementations
Different catalogs have different capabilities:
- **HiveCatalog**: Uses Hive Metastore, production-ready
- **HadoopCatalog**: File-system based, good for testing
- **RESTCatalog**: HTTP-based, supports custom implementations
- **NessieCatalog**: Git-like version control for tables

### Performance Considerations
- Table metadata operations should be minimized
- Manifest file caching is critical for read performance
- Use appropriate file formats based on workload (Parquet for analytics, ORC for Hive compatibility)

### Integration Testing
Integration tests require Docker and test against real services:
- Hive Metastore
- AWS S3 (via LocalStack)
- Apache Kafka
- Various SQL engines

To skip integration tests during development:
```bash
./gradlew build -x integrationTest
```