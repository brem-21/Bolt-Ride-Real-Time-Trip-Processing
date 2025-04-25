install:
	pip install --upgrade pip && \
	pip install -r requirements.txt

format:
	black scripts/*.py 

test:
	python -m pytest -vv --cov=scripts scripts/test_*.py

refactor: format lint

all: install format test 