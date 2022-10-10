# About Mitzu

Mitzu is a python based product analytics tool. Mitzu is an open source alternative to these tools.
- [Amplitude](https://amplitude.com/)
- [Mixpanel](https://mixpanel.com/)
- [Heap](https://heap.io/)
- etc...

Mitzu connects directly to the company's data warehouse or data lake. The users of Mitzu can 

# Setting Up Local Environment

Before you begin you need to have these packages installed:
- [poetry](https://python-poetry.org/)
- [docker](https://www.docker.com/)
- [docker-compose](https://docs.docker.com/compose/)

We use `Makefile` to do most of the operations around the project.


### Install Dependencies

```bash
make init
```

### Testing Locally with Docker

```bash
make docker_test_up # start docker for integration tests
make setup_test_data # create dummy data for integration tests

make test_coverage

# optionally:
# make docker_test_down # stop docker
```

### Linting and Syntax Checks

```bash
make mypy lint
```

### Code Formatting

```bash
make format autoflake
```

### Setting up webapp locally

For this the best is following the example [here](examples/webapp-docker/README.md)