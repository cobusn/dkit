#
# Makefile
#
PYTHON=python3

all: sdist bdist

sdist:
	$(PYTHON) setup.py sdist

install:
	$(PYTHON) setup.py install

bdist:
	$(PYTHON) setup.py bdist_wininst

clean:
	rm -rf build dist
