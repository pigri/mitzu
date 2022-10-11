# About Mitzu

Mitzu is a python based product analytics tool. Mitzu is an open source alternative to these tools:
- [Amplitude](https://amplitude.com/)
- [Mixpanel](https://mixpanel.com/)
- [Heap](https://heap.io/)
- etc...

Mitzu connects directly to the company's data warehouse or data lake.

# Setting Up Local Environment

Before you begin you need to have these packages installed:
- [poetry](https://python-poetry.org/)
- [docker](https://www.docker.com/)
- [docker-compose](https://docs.docker.com/compose/)

We use `Makefile` to do most of the operations around the project.


## Install Dependencies

```bash
make init
```

## Testing Locally with Docker

```bash
make docker_test_up # start docker for integration tests
make setup_test_data # create dummy data for integration tests

make test_coverage

# optionally:
make docker_test_down # stop docker
```

## Linting and Syntax Checks

```bash
make mypy lint
```

## Code Formatting

```bash
make format autoflake
```

# Running Locally

You can run Mitzu locally in 3 ways:

- Standalone webapp
- Low-code notebook commands
- Embedded notebook app

To run any of these first you need to have a running `data warehouse or data lake` and a `Mitzu Project`. We have already created the `test data lake` with these two steps:

```bash
make docker_test_up
make setup_test_data
```

These will create a data lake consisting of `Minio + Hive + Trino`.
The test project that can attach to the test data lake is at  `./trino_test_project.mitzu`.

## Running Standalone Webapp

Start the webapp with this command:
```bash
make dash_simple
```

This will start the webapp at http://localhost:8082.

## Running Notebook Commands and Notebook App

Start the local prepared jupyter hub with this command:

```bash
make notebook
```
Navigate to `examples/notebook/trino_test_project.ipynb`


## Understand Mitzu

Please our [Docs](DOCS.md) before contributing