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
	# TBD: Setup Minio, data has to be uploaded to minio
	docker container exec -it docker_trino-coordinator_1 trino --execute="$$(cat tests/integration/docker/trino_hive_init.sql)"

test_coverage:
	$(POETRY) run  pytest --cov=mitzu --cov-report=html tests/

check: format autoflake mypy lint test_coverage
	@ECHO 'done'

notebook: 
	$(POETRY) run jupyter lab

dash: 	
	cd release && \
	BASEPATH=../examples/webapp-docker/basepath/ \
	LOG_LEVEL=INFO \
	LOG_HANDLER=stdout \
	DASH_REQUESTS_PATHNAME_PREFIX="/Prod/" \
	DASH_ROUTES_PATHNAME_PREFIX="/Prod/" \
	MANAGE_PROJECTS_LINK=http://localhost:8081 \
	$(POETRY) run gunicorn -b 0.0.0.0:8082 app:server --reload

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
	docker image build ./release --platform=linux/amd64 \
	-t imeszaros/mitzu-webapp:$(shell poetry version -s) \
	-t imeszaros/mitzu-webapp:latest \
	--build-arg ADDITIONAL_DEPENDENCIES="mitzu==$(shell poetry version -s) databricks-sql-connector==2.0.2 trino==0.313.0 PyAthena==2.13.0"
	
docker_publish_no_build:
	docker push imeszaros/mitzu-webapp	

docker_publish: docker_build
	make docker_publish_no_build



