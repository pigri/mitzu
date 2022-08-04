POETRY := $(shell command -v poetry 2> /dev/null)

clean:
	$(POETRY) run pyclean mitzu release tests 
	rm -rf dist htmlcov
	rm -rf .pytest_cache
	rm -rf .mypy_cache
	rm -rf .ipynb_checkpoints

format: ## formats all python code
	$(POETRY) run black mitzu tests release

lint: ## lints and checks formatting all python code
	$(POETRY) run black --check mitzu tests release
	$(POETRY) run flake8 mitzu tests release

autoflake: ## fixes imports, unused variables
	$(POETRY) run autoflake -r -i --remove-all-unused-imports --remove-unused-variables --expand-star-imports mitzu/ tests/  release/

mypy:
	$(POETRY) run mypy mitzu tests release --ignore-missing-imports 

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
	docker container exec -it docker-trino-coordinator-1 trino --execute="$$(cat tests/integration/docker/trino_hive_init.sql)"

test_coverage:
	$(POETRY) run  pytest --cov=mitzu --cov-report=html tests/

check: format autoflake mypy lint test_coverage
	@ECHO 'done'

notebook: 
	$(POETRY) run jupyter lab

dash: 	
	cd release && \
	BASEPATH=../example/basepath/ \
	MANAGE_PROJECTS_LINK=http://localhost:8081 \
	$(POETRY) run gunicorn -b 0.0.0.0:8082 app:server


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

docker_build:
	docker image build ./release \
	-t imeszaros/mitzu-webapp:$(shell poetry version -s) \
	-t imeszaros/mitzu-webapp:latest \
	--build-arg ADDITIONAL_DEPENDENCIES="mitzu==$(shell poetry version -s) databricks-sql-connector==2.0.2 trino==0.313.0"
	
docker_build_athena:	
	docker image build ./release \
	-t imeszaros/mitzu-webapp-athena:$(shell poetry version -s) \
	-t imeszaros/mitzu-webapp-athena:latest \
	--build-arg ADDITIONAL_DEPENDENCIES="mitzu==$(shell poetry version -s) PyAthena==1.11.5"	

docker_build_all: docker_build docker_build_athena
	@echo "Done building all"

docker_publish_no_build:
	docker push imeszaros/mitzu-webapp
	docker push imeszaros/mitzu-webapp-athena

docker_publish: docker_build_all
	make docker_publish_no_build



