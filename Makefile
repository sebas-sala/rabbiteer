build:
	rm -rf dist
	pip install --upgrade build
	python -m build

upload:
	pip install --upgrade twine
	twine upload dist/*
	