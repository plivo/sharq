.PHONY: all clean build install uninstall test

all: clean

clean:
	find . -name \*.pyc -delete
	find . -name \*.pyo -delete
	find . -name \*~ -delete
	rm -rf dist SharQ.egg-info

build:
	python setup.py sdist

install:
	pip install dist/SharQ-*.tar.gz

uninstall:
	yes | pip uninstall sharq

test:
	python -m tests.test_queue
	python -m tests.test_func
