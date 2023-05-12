POETRY := $(shell command -v poetry 2> /dev/null)

clean:
	$(POETRY) run pyclean mitzu release tests 
	rm -rf dist
	rm -rf htmlcov
	rm -rf .pytest_cache
	rm -rf .mypy_cache
	rm -rf .ipynb_checkpoints
	rm -f coverage.xml
	rm -rf .hypothesis
	rm -rf storage

init:
	$(POETRY) install --all-extras
	$(POETRY) self add "poetry-dynamic-versioning[plugin]"

format: ## formats all python code
	$(POETRY) run autoflake -r -i --remove-all-unused-imports --remove-unused-variables --expand-star-imports mitzu/ tests/ release/
	$(POETRY) run black mitzu tests release 

lint: ## lints and checks formatting all python code
	$(POETRY) run black --exclude .dbs --check mitzu tests release
	$(POETRY) run flake8 mitzu tests release
	$(POETRY) run mypy mitzu tests release --ignore-missing-imports 
	

test_units:
	$(POETRY) run pytest -sv tests/unit/

test_hypothesis: ## runs all property based tests with a bigger example size
	HYPOTHESIS_MAX_EXAMPLES=100 $(POETRY) run pytest -sv $(shell grep -r "tests.unit.webapp.generators" tests/ 2>/dev/null | cut -d ":" -f 1)

test_integrations:
	docker-compose -f docker/docker-compose.yml up -d --no-recreate
	$(POETRY) run pytest -sv tests/integration/

test_project_creation_and_discovery:
	$(POETRY) run python3 scripts/create_example_project.py --project-dir . --overwrite-records --adapter postgresql
	$(POETRY) run python3 scripts/create_example_project.py --project-dir . --overwrite-records --adapter mysql


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
	$(POETRY) run pytest --cov=mitzu --cov-report=html --cov-report=xml tests/

check: lint test_coverage
	@echo 'done'


# This make command is used for testing the SSO
serve_cognito_sso:
	cd release/app/ && \
	LOG_LEVEL=INFO \
	LOG_HANDLER=stdout \
	AUTH_BACKEND="cognito" \
	COGNITO_CLIENT_ID="1bqlja23lfmniv7bm703aid9o0" \
	COGNITO_CLIENT_SECRET="${COGNITO_CLIENT_SECRET}" \
	COGNITO_DOMAIN="signin.mitzu.io" \
	COGNITO_REGION="eu-west-1" \
	COGNITO_POOL_ID="eu-west-1_QkZu6BnVD" \
	COGNITO_REDIRECT_URL="http://localhost:8082/auth/oauth" \
	SETUP_SAMPLE_PROJECT='true' \
	$(POETRY) run gunicorn -b 0.0.0.0:8082 app:server --reload

serve_google_sso:
	cd release/app/ && \
	LOG_LEVEL=INFO \
	LOG_HANDLER=stdout \
	AUTH_BACKEND="google" \
	GOOGLE_CLIENT_ID="669095060108-42hhm4rgo8cjseumiu47saq2g8690ehh.apps.googleusercontent.com" \
	GOOGLE_CLIENT_SECRET="${GOOGLE_CLIENT_SECRET}" \
	GOOGLE_PROJECT_ID="mitzu-test" \
	GOOGLE_REDIRECT_URL="http://localhost:8082/auth/oauth" \
	$(POETRY) run gunicorn -b 0.0.0.0:8082 app:server --reload

serve:
	cd release/app/ && \
	LOG_LEVEL=WARN \
	LOG_HANDLER=stdout \
	AUTH_BACKEND="local" \
	AUTH_ROOT_PASSWORD="testuser" \
	$(POETRY) run gunicorn -b 0.0.0.0:8082 app:server --reload --workers=8

run:	
	LOG_LEVEL=INFO \
	LOG_HANDLER=stdout \
	SETUP_SAMPLE_PROJECT=true \
	AUTH_BACKEND="local" \
	AUTH_ROOT_PASSWORD="testuser" \
	AUTH_ROOT_USER_EMAIL="root@local" \
	$(POETRY) run python mitzu/webapp/webapp.py

build: check
	$(POETRY) build

bump_version:
	$(POETRY) version patch

publish: bump_version build
	$(POETRY) publish

publish_no_build:
	$(POETRY) publish

docker_build_latest:	
	$(POETRY) build
	cp -r ./dist/ ./release/dist/
	poetry export \
		-E webapp \
		-E trinodwh \
		-E postgres \
		-E databricks \
		-E athena \
		-E snowflake \
		-E mysql \
		-E redshift \
		-E bigquery \
		--without-hashes \
		--format=requirements.txt > release/requirements.txt	
	docker build ./release \
	--platform "linux/amd64" \
	--build-arg MITZU_VERSION=$(shell poetry version -s) \
	-f ./release/Dockerfile \
	-t mitzuio/mitzu:$(shell poetry version -s) \
	-t mitzuio/mitzu:latest

docker_build_amd64_snapshot:
	$(POETRY) build
	cp -r ./dist/ ./release/dist/
	poetry export \
		-E webapp \
		-E trinodwh \
		-E postgres \
		-E databricks \
		-E athena \
		-E snowflake \
		-E mysql \
		-E redshift \
		-E bigquery \
		--without-hashes \
		--format=requirements.txt > release/requirements.txt	
	docker build ./release \
	--platform "linux/amd64" \
	--build-arg MITZU_VERSION=$(shell poetry version -s) \
	-f ./release/Dockerfile \
	-t mitzuio/mitzu:snapshot

docker_run_amd64_snapshot:
	rm -rf ./docker_cache/
	docker run -v "$(pwd)/docker_cache/:/app/cache" -e SETUP_SAMPLE_PROJECT=false KALEIDO_CONFIGS="" -e LOCAL_CACHING_ENABLED=false -p 8082:8080 mitzuio/mitzu:snapshot

docker_publish_latest: docker_build_latest
	docker push mitzuio/mitzu:$(shell poetry version -s)
	docker push mitzuio/mitzu:latest

docker_publish_no_build:
	docker push mitzuio/mitzu:$(shell poetry version -s)
	docker push mitzuio/mitzu:latest

docker_publish_snapshot_no_build:
	docker push mitzuio/mitzu:snapshot



