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
	$(POETRY) run autoflake -r -i --remove-all-unused-imports --remove-unused-variables --expand-star-imports mitzu/ tests/ release/
	$(POETRY) run black mitzu tests release *.ipynb

lint: ## lints and checks formatting all python code
	$(POETRY) run black --exclude .dbs --check mitzu tests release
	$(POETRY) run flake8 mitzu tests release

mypy:
	$(POETRY) run mypy mitzu tests release --ignore-missing-imports 

test_units:
	$(POETRY) run pytest -sv tests/unit/

test_integrations:
	docker-compose -f docker/docker-compose.yml up -d --no-recreate
	$(POETRY) run pytest -sv tests/integration/

test_project_creation_and_discovery:
	$(POETRY) run python3 scripts/create_example_project.py --project-dir . --overwrite-records --adapter postgresql
	$(POETRY) run python3 scripts/create_example_project.py --project-dir . --overwrite-records --adapter mysql

test_notebooks: test_project_creation_and_discovery
	sh scripts/convert_and_run_notebook.sh

docker_test_down:
	rm -rf tests/.dbs/
	docker-compose -f docker/docker-compose.yml down

docker_test_up:	
	docker-compose -f docker/docker-compose.yml up -d 

generate_docs:
	$(POETRY) run sphinx-build docs docs/build

generate_docs_ci:
	$(POETRY) run sphinx-build -W docs docs/build

trino_setup_test_data:
	$(POETRY) run python3 scripts/wait_for_trino.py
	docker container exec docker-trino-coordinator-1 trino --execute="$$(cat docker/trino_hive_init.sql)"
	
test_coverage:
	$(POETRY) run pytest --cov=mitzu --cov-report=html tests/

check: lint mypy test_coverage test_notebooks
	@echo 'done'

test_coverage_ci:
	$(POETRY) run pytest --cov=mitzu --cov-report=xml tests/


notebook: 
	$(POETRY) run jupyter lab

# This make command is used for testing the SSO
serve_cognito_sso:
	cd release/app/ && \
	LOG_LEVEL=INFO \
	LOG_HANDLER=stdout \
	MANAGE_PROJECTS_LINK="http://localhost:8081" \
	MITZU_WEBAPP_URL="http://localhost:8082" \
	HOME_URL="http://localhost:8082" \
	OAUTH_BACKEND="cognito" \
	COGNITO_CLIENT_ID="1bqlja23lfmniv7bm703aid9o0" \
	COGNITO_CLIENT_SECRET="${COGNITO_CLIENT_SECRET}" \
	COGNITO_DOMAIN="signin.mitzu.io" \
	COGNITO_REGION="eu-west-1" \
	COGNITO_POOL_ID="eu-west-1_QkZu6BnVD" \
	COGNITO_REDIRECT_URL="http://localhost:8082/auth/oauth" \
	$(POETRY) run gunicorn -b 0.0.0.0:8082 app:server --reload

serve_google_sso:
	cd release/app/ && \
	LOG_LEVEL=INFO \
	LOG_HANDLER=stdout \
	MANAGE_PROJECTS_LINK="http://localhost:8081" \
	MITZU_WEBAPP_URL="http://localhost:8082" \
	HOME_URL="http://localhost:8082" \
	OAUTH_BACKEND="google" \
	GOOGLE_CLIENT_ID="669095060108-42hhm4rgo8cjseumiu47saq2g8690ehh.apps.googleusercontent.com" \
	GOOGLE_CLIENT_SECRET="${GOOGLE_CLIENT_SECRET}" \
	GOOGLE_PROJECT_ID="mitzu-test" \
	GOOGLE_REDIRECT_URL="http://localhost:8082/auth/oauth" \
	$(POETRY) run gunicorn -b 0.0.0.0:8082 app:server --reload

serve:
	cd release/app/ && \
	LOG_LEVEL=WARN \
	LOG_HANDLER=stdout \
	MANAGE_PROJECTS_LINK="http://localhost:8081" \
	HOME_URL="http://localhost:8082/" \
	$(POETRY) run gunicorn -b 0.0.0.0:8082 app:server --reload

run:
	$(POETRY) run python mitzu/webapp/webapp.py

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



