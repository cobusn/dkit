#
# Makefile for dkit project 
#
PYTHON=python3
TAG := $(shell $(PYTHON) -c 'import dkit; print(dkit.__version__)')
# export SPHINXBUILD=/cygdrive/c/Anaconda/envs/py36/Scripts/sphinx-build

.PHONY: test clean

all: build

docker: Dockerfile Makefile
	docker build -t dkit:latest .
	docker tag dkit:latest dkit:$(TAG)

test:
	cd test && \
		pytest --cov=dkit --cov=lib_dk &&\
		coverage html &&\
		coverage report

build:
	$(PYTHON) -m build

sdist:
	$(PYTHON) -m build --sdist

wheel:
	$(PYTHON) -m build --wheel

install:
	pip install --user .

clean:
	rm -rf build dist *.egg-info
	find . | grep \.pyc$ | xargs rm -fr
	find . | grep __pycache__ | xargs rm -rf
	rm -f MANIFEST
	rm -f dkit/data/*.c
	rm -f dkit/utilities/*.c
	rm -f dkit/{doc,data,utilities}/*.pyc
	rm -f {dkit,test}/*.pyc
	rm -rf test/cover
	rm -rf {dkit,test}/__pycache__
