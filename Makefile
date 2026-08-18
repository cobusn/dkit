#
# Makefile for dkit project 
#
PYTHON=python3
TAG := $(shell $(PYTHON) -c 'import dkit; print(dkit.__version__)')
# export SPHINXBUILD=/cygdrive/c/Anaconda/envs/py36/Scripts/sphinx-build

.PHONY: test testdata clean doc

all: build

docker: Dockerfile Makefile
	docker build -t dkit:latest .
	docker tag dkit:latest dkit:$(TAG)

test/input_files/sample.jsonl:
	cd test && $(PYTHON) create_data.py

testdata: test/input_files/sample.jsonl

test: test/input_files/sample.jsonl
	cd test && \
		pytest --cov=dkit --cov=lib_dk &&\
		coverage html &&\
		coverage report

doc: examples/*.py doc/source/*.rst Makefile
	+cd examples && $(MAKE) cleanfiles
	+cd examples && $(MAKE)
	+cd doc && $(MAKE) html \
		&& cd .. \
		&& cp -r doc/build/* html

build:
	$(PYTHON) -m build

sdist:
	$(PYTHON) -m build --sdist

wheel:
	$(PYTHON) -m build --wheel

install:
	pip install .

clean:
	rm -rf build dist *.egg-info
	find . | grep \.pyc$ | xargs rm -fr
	find . | grep __pycache__ | xargs rm -rf
	rm -f MANIFEST
	rm -f dkit/data/*.c
	rm -f dkit/utilities/*.c
	rm -f tdigest/*.c
	rm -f dkit/data/*.so
	rm -f dkit/utilities/*.so
	rm -f tdigest/*.so
	rm -f dkit/{doc,data,utilities}/*.pyc
	rm -f {dkit,test}/*.pyc
	rm -rf test/cover
	rm -rf {dkit,test}/__pycache__
