# Tests for ETL Pipeline

This directory contains unit tests for the ETL pipeline scripts.

## Running Tests

Run all tests:
```bash
python -m unittest discover tests
```

Run specific test file:
```bash
python -m unittest tests.test_validators
python -m unittest tests.test_permissions_check
python -m unittest tests.test_verify_dependencies
```

## Test Coverage

- `test_validators.py`: Tests for environment variable validation
- `test_permissions_check.py`: Tests for permission verification functions
- `test_verify_dependencies.py`: Tests for dependency checking

## Requirements

Tests require the following packages:
- unittest (built-in)
- unittest.mock (built-in)
