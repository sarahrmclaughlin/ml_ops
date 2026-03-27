style:
	black src/ tests/ dags/ *.py
	isort src/ tests/ dags/ *.py
	flake8 src/ tests/ dags/ *.py
	pylint src/ tests/ dags/ *.py
	pydocstyle src/ tests/ dags/ *.py

check-style:
	black --check src/  *.py
	isort --check src/ *.py
	flake8 src/ *.py
	pylint src/ *.py
	pydocstyle src/  *.py