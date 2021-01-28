#
# Makefile for dkit project 
#
all: 
	nosetests \
		--logging-level=ERROR \
		--with-coverage \
		--cover-erase \
		--cover-package=dkit \
		--cover-html \
		test_*.py

clean:
	rm -f *.pyc
	rm -f *.log
	rm -rf htmlcov
	rm -rf .coverage
	rm -rf plots/*.svg
