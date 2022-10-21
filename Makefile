POETRY := $(shell command -v poetry 2> /dev/null)
CREATE_TEST_DATA_CMD = docker container exec docker_trino-coordinator_1 trino --execute="$$(cat tests/integration/docker/trino_hive_init.sql)"

clean:
	$(POETRY) run pyclean mitzu release tests 
	rm -rf dist htmlcov
	rm -rf .pytest_cache
	rm -rf .mypy_cache
	rm -rf .ipynb_checkpoints

init:
	$(POETRY) install -E mysql -E trinodwh -E webapp -E postgres


format: ## formats all python code
	$(POETRY) run black mitzu tests release

lint: ## lints and checks formatting all python code
	$(POETRY) run black --exclude .dbs --check mitzu tests release
	$(POETRY) run flake8 mitzu tests release

autoflake: ## fixes imports, unused variables
	$(POETRY) run autoflake -r -i --remove-all-unused-imports --remove-unused-variables --expand-star-imports mitzu/ tests/ release/

mypy:
	$(POETRY) run mypy mitzu tests release --ignore-missing-imports 

test_units:
	$(POETRY) run pytest -sv tests/unit/

test_integrations:
	docker-compose -f tests/integration/docker/docker-compose.yml up -d --no-recreate
	$(POETRY) run pytest -sv tests/integration/

docker_test_down:
	rm -rf tests/.dbs/
	docker-compose -f tests/integration/docker/docker-compose.yml down

docker_test_up:	
	docker-compose -f tests/integration/docker/docker-compose.yml up

setup_test_data:
	# TBD: Setup Minio, data has to be uploaded to minio
	for i in {1..12}; do $(CREATE_TEST_DATA_CMD) && break || sleep 5; done
	
test_coverage:
	$(POETRY) run pytest --cov=mitzu --cov-report=html tests/

check: format autoflake mypy lint test_coverage
	@ECHO 'done'

test_coverage_ci:
	$(POETRY) run pytest --cov=mitzu --cov-report=xml tests/

check_ci: autoflake mypy lint test_coverage_ci
	@echo 'done'

notebook: 
	$(POETRY) run jupyter lab

dash: 	
	cd release/app/ && \
	BASEPATH=../../examples/webapp-docker/mitzu/ \
	LOG_LEVEL=DEBUG \
	LOG_HANDLER=stdout \
	MANAGE_PROJECTS_LINK="http://localhost:8081" \
	MITZU_WEBAPP_URL="http://localhost:8082/" \
	HOME_URL="http://localhost:8082/" \
	NOT_FOUND_URL="http://localhost:8082/not_found" \
	SIGN_OUT_URL="http://localhost:8082/logout" \
	OAUTH_SIGN_IN_URL="https://signin.mitzu.io/oauth2/authorize?client_id=1bqlja23lfmniv7bm703aid9o0&response_type=code&scope=email+openid&redirect_uri=http://localhost:8082/" \
	OAUTH_JWT_AUDIENCE=1bqlja23lfmniv7bm703aid9o0 \
	OAUTH_REDIRECT_URI="http://localhost:8082/" \
	OAUTH_CLIENT_ID=1bqlja23lfmniv7bm703aid9o0 \
	OAUTH_CLIENT_SECRET="${OAUTH_CLIENT_SECRET}" \
	OAUTH_TOKEN_URL="https://signin.mitzu.io/oauth2/token" \
	OAUTH_AUTHORIZED_EMAIL_REG="." \
	OAUTH_JWKS_URL="https://cognito-idp.eu-west-1.amazonaws.com/eu-west-1_QkZu6BnVD/.well-known/jwks.json" \
	OAUTH_SIGN_OUT_REDIRECT_URL="https://signin.mitzu.io/logout" \
	$(POETRY) run gunicorn -b 0.0.0.0:8082 app:server --reload

dash_simple: 	
	cd release/app/ && \
	BASEPATH=../../examples/webapp-docker/mitzu/ \
	LOG_LEVEL=INFO \
	LOG_HANDLER=stdout \
	MANAGE_PROJECTS_LINK="http://localhost:8081" \
	HOME_URL="http://localhost:8082/" \
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
	--build-arg ADDITIONAL_DEPENDENCIES="mitzu[webapp,databricks,trinodwh,athena]==$(shell poetry version -s)" --no-cache

docker_build_local:
	cp -r ./dist/ ./release/dist/
	poetry export -E trinodwh -E postgresql -E webapp -E databricks --without-hashes --format=requirements.txt > release/requirements.txt
	docker image build ./release \
	--platform=linux/amd64 \
	--build-arg MITZU_VERSION=$(shell poetry version -s) \
	--build-arg DIST_PATH=$(shell pwd)/dist/ \
	--no-cache -f ./release/LocalDockerfile \
	-t imeszaros/mitzu-webapp:$(shell poetry version -s) \
	-t imeszaros/mitzu-webapp:latest


docker_publish_no_build:
	docker push imeszaros/mitzu-webapp	

docker_publish: docker_build
	make docker_publish_no_build



