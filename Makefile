POETRY := $(shell command -v poetry 2> /dev/null)


format: ## formats all python code
	$(POETRY) run black services tests

lint: ## lints and checks formatting all python code
	$(POETRY) run black --check services tests
	$(POETRY) run flake8 services tests

autoflake: ## fixes imports, unused variables
	$(POETRY) run autoflake -r -i --remove-all-unused-imports --remove-unused-variables --expand-star-imports services/ tests/

mypy:
	$(POETRY) run mypy services tests

test:
	$(POETRY) run pytest -sv tests/

check: format autoflake mypy lint test
	ECHO 'done'

notebook:
	$(POETRY) run jupyter-notebook