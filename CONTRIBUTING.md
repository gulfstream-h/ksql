Конечно, вот версия `CONTRIBUTING.md` без эмодзи:

---

# Contributing to `ksql`

Thank you for your interest in contributing to `ksql`. This guide outlines how to get started and the conventions to follow when contributing code, tests, or documentation.

## Getting Started

1. Fork the repository and clone it:

   ```bash
   git clone https://github.com/your-org/ksql.git
   cd ksql
   ```

2. Make sure you have Go 1.24+ installed.

3. Install dependencies:

   ```bash
   go mod tidy
   ```

4. Run tests:

   ```bash
   go test ./...
   ```

## CI Pipelines

We use CI pipelines to ensure code quality and release safety:

* **Test pipeline**: runs all unit and integration tests
* **Release build check**: ensures the codebase can be successfully built and packaged as a release, without actually publishing

These pipelines run automatically on push and pull requests. Make sure they pass before requesting a review.

## Coding Guidelines

* Write clean, idiomatic Go code

* Format code using `go fmt` or `gofmt`

* Run linters before committing:

  ```bash
  golangci-lint run
  ```

* Add tests for any new functionality or bug fixes

* Keep functions and packages small and focused

## Pull Request Checklist

* [ ] Clear, descriptive title and summary
* [ ] One feature or fix per pull request
* [ ] Includes appropriate test coverage
* [ ] Passes `go test ./...` locally
* [ ] CI pipelines are passing
* [ ] Code is formatted and linted

## Project Structure

* `internal/` – private packages
* `cmd/cli` – command-line tools 
* `streams/` - user stream API
* `tables/` - user table API
* `topics` - user topic API 
* `ksql/` - ksql builder functionality
* `database/` - query-mode user API
* `internal/kernel` - core functionality
* `internal/schema` - schema-reflection package
* `internal/reflector` - reflect functions 

## Running Tests

To run all tests:

```bash
go test ./...
```

To run a specific test or package:

```bash
go test ./ksql -run Test_AggregateFn 
```

## Questions and Discussion

If you encounter issues or have suggestions, feel free to open an issue. For broader discussions, use [GitHub Discussions](https://github.com/your-org/ksql/discussions).

---

Хочешь адаптировать это под монорепу или добавить секцию про релизы/ветвление?
