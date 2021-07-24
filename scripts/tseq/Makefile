#
# Makefile
#
PYTHON=python3

all: sdist tseq.1

tseq.1: tseq.1.md
	pandoc tseq.1.md -s -t man -o tseq.1

sdist:
	$(PYTHON) setup.py sdist

install:
	$(PYTHON) setup.py install

clean:
	rm -rf build dist
	rm -rf __pycache__
	rm -rf *.egg-info
	rm -rf tseq.1
