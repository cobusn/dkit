#
# Makefile for dkit project 
#

PYTHON=python
# export SPHINXBUILD=/cygdrive/c/Anaconda/envs/py36/Scripts/sphinx-build

.PHONY: test

all: sdist bdist

test:
	cd test && make -j 7

doc: examples/*.py doc/images/Makefile doc/source/*.rst Makefile
	cd doc/images && make -j 8
	cd examples && make cleanfiles
	cd examples && make
	cd doc && make html \
		&& cd .. \
		&& cp -r doc/build/* html


sdist:
	$(PYTHON) setup.py sdist

install:
	$(PYTHON) setup.py install

bdist:
	$(PYTHON) setup.py bdist_wininst

clean:
	python setup.py clean --all
	cd doc && make clean
	cd doc/images && make clean
	cd examples && make clean
	cd scripts/dk && make clean
	cd scripts/pyeek && make clean
	cd scripts/xpstat && make clean
	cd scripts/vigenere && make clean
	cd scripts/tseq && make clean
	cd test && make clean
	find . | grep \.pyc$ | xargs rm -fr
	find . | grep __pycache__ | xargs rm -rf
	rm -f MANIFEST
	rm -rf dkit.egg-info
	rm -f dkit/data/*.c
	rm -f dkit/utilities/*.c
	rm -f dkit/{doc,data,utilities}/*.pyc  
	rm -f {dkit,test}/*.pyc
	rm -rf build dist
	rm -rf test/cover
	rm -rf {dkit,test}/__pycache__
