POETRY := $(shell command -v poetry 2> /dev/null)


format: ## formats all python code
	$(POETRY) run black mitzu tests serverless

lint: ## lints and checks formatting all python code
	$(POETRY) run black --check mitzu tests serverless
	$(POETRY) run flake8 mitzu tests serverless

autoflake: ## fixes imports, unused variables
	$(POETRY) run autoflake -r -i --remove-all-unused-imports --remove-unused-variables --expand-star-imports mitzu/ tests/  serverless/

mypy:
	$(POETRY) run mypy mitzu tests serverless --ignore-missing-imports 

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
	$(POETRY) run python serverless/app/test_app.py

dash_profile: 
	$(POETRY) run python serverless/app/test_app.py --profile

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
	docker image build -t mitzu-demo-app ./serverless/app/

sam_local:
	$(POETRY) install && \
	cd serverless && \
	sam build && \
	sam local start-api && \
	cd ../

deploy_sam:
	cd serverless && \
	sam build && \
	sam deploy && \
	cd ../

