POETRY := $(shell command -v poetry 2> /dev/null)


format: ## formats all python code
	$(POETRY) run black mitzu tests

lint: ## lints and checks formatting all python code
	$(POETRY) run black --check mitzu tests
	$(POETRY) run flake8 mitzu tests

autoflake: ## fixes imports, unused variables
	$(POETRY) run autoflake -r -i --remove-all-unused-imports --remove-unused-variables --expand-star-imports mitzu/ tests/

mypy:
	$(POETRY) run mypy mitzu tests --ignore-missing-imports

unit_tests:
	$(POETRY) run pytest -sv tests/unit/

test_integrations:
	docker-compose -f tests/integration/docker/docker-compose.yml up -d --no-recreate
	$(POETRY) run pytest -sv tests/integration/

docker_test_down:
	rm -rf tests/.dbs/
	docker-compose -f tests/integration/docker/docker-compose.yml down

docker_test_up:	
	docker-compose -f tests/integration/docker/docker-compose.yml up	

trino_setup_test_data:
	docker container exec -it docker_trino-coordinator_1 trino --execute="$$(cat tests/integration/docker/trino_hive_init.sql)"

test_coverage:
	$(POETRY) run  pytest --cov=mitzu --cov-report=html tests/

check: format autoflake mypy lint test_coverage
	@ECHO 'done'

notebook:
	$(POETRY) run jupyter-notebook

build: check
	$(POETRY) build

generate_test_data:
	$(POETRY) run python scripts/create_test_data.py $(SIZE)

bump_version:
	$(POETRY) version patch

publish: bump_version build
	$(POETRY) publish

publish_no_build:
	$(POETRY) publish