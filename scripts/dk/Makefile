all:	test

sdist:
	python3 setup.py sdist

install:
	python3 setup.py install

bdist:
	python3 setup.py bdist_wininst

doc: doc/images/Makefile doc/source/*.rst lib_dk/*.py Makefile
	cd doc/images && make
	cd doc && make html

clean:
	rm -rf dist
	rm -rf build	
	rm -f *.pyc
	rm -rf *.egg-info
	find . | grep __pycache__ | xargs rm -rf
