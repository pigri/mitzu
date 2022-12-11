POETRY := $(shell command -v poetry 2> /dev/null)

clean:
	$(POETRY) run pyclean mitzu release tests 
	rm -rf dist htmlcov
	rm -rf .pytest_cache
	rm -rf .mypy_cache
	rm -rf .ipynb_checkpoints

init:
	$(POETRY) install -E mysql -E trinodwh -E webapp -E postgres -E athena -E snowflake  -E databricks

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
	docker-compose -f docker/docker-compose.yml up -d --no-recreate
	$(POETRY) run pytest -sv tests/integration/

docker_test_down:
	rm -rf tests/.dbs/
	docker-compose -f docker/docker-compose.yml down

docker_test_up:	
	docker-compose -f docker/docker-compose.yml up -d 

trino_setup_test_data:
	$(POETRY) run python3 scripts/wait_for_trino.py
	docker container exec docker-trino-coordinator-1 trino --execute="$$(cat docker/trino_hive_init.sql)"
	
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

# This make command is used for testing the SSO
dash_trino_sso: 	
	cd release/app/ && \
	BASEPATH=../../examples/data/ \
	LOG_LEVEL=INFO \
	LOG_HANDLER=stdout \
	MANAGE_PROJECTS_LINK="http://localhost:8081" \
	MITZU_WEBAPP_URL="http://localhost:8082/" \
	HOME_URL="http://localhost:8082/" \
	NOT_FOUND_URL="http://localhost:8082/not_found" \
	SIGN_OUT_URL="http://localhost:8082/logout" \
	AUTH_BACKEND="cognito" \
	COGNITO_CLIENT_ID="1bqlja23lfmniv7bm703aid9o0" \
	COGNITO_CLIENT_SECRET="${COGNITO_CLIENT_SECRET}" \
	COGNITO_DOMAIN="signin.mitzu.io" \
	COGNITO_REGION="eu-west-1" \
	COGNITO_POOL_ID="eu-west-1_QkZu6BnVD" \
	COGNITO_REDIRECT_URL="http://localhost:8082/" \
	$(POETRY) run gunicorn -b 0.0.0.0:8082 app:server --reload

serve:
	cd release/app/ && \
	LOG_LEVEL=WARN \
	LOG_HANDLER=stdout \
	MANAGE_PROJECTS_LINK="http://localhost:8081" \
	HOME_URL="http://localhost:8082/" \
	$(POETRY) run gunicorn -b 0.0.0.0:8082 app:server --reload

build: check
	$(POETRY) build

bump_version:
	$(POETRY) version patch

publish: bump_version build
	$(POETRY) publish

publish_no_build:
	$(POETRY) publish

docker_build:	
	docker image build ./release --platform="linux/amd64" \
	-t imeszaros/mitzu-webapp:$(shell poetry version -s) \
	-t imeszaros/mitzu-webapp:latest \
	--build-arg ADDITIONAL_DEPENDENCIES="mitzu[webapp,databricks,trinodwh,athena]==$(shell poetry version -s)" --no-cache

docker_build_local:
	cp -r ./dist/ ./release/dist/
	poetry export -E trinodwh -E postgresql -E webapp -E databricks --without-hashes --format=requirements.txt > release/requirements.txt
	docker image build ./release \
	--platform="linux/amd64" \
	--build-arg MITZU_VERSION=$(shell poetry version -s) \
	--build-arg DIST_PATH=$(shell pwd)/dist/ \
	--no-cache -f ./release/LocalDockerfile \
	-t mitzu-webapp

docker_publish_no_build:
	docker push imeszaros/mitzu-webapp	

docker_publish: docker_build
	make docker_publish_no_build



