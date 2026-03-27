style:
	black src/ tests/ dags/ *.py

check-style:
	black --check src/ tests/ *.py
	isort --check src/ tests/ dags/ *.py
	flake8 src/ tests/ dags/ *.py
	pylint src/ tests/ dags/ *.py
	pydocstyle src/ tests/ dags/ *.py