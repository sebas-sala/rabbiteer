build:
	rm -rf dist
	pip install --upgrade build
	python3 -m build

upload:
	pip install --upgrade twine
	twine upload dist/*
	