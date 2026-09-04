# PYTHON
CONFIGPY    = example_configured_application.py
CONFIGURED  = $(CONFIGPY:.py=.out)
PYTHONFILES = $(filter-out $(CONFIGPY), $(wildcard *.py))
OUTFILES    = $(PYTHONFILES:.py=.out)

PYTHON	    = python

all:	$(CONFIGURED) $(OUTFILES) $(OUTTEX)

$(CONFIGURED) : %.out : %.py
	$(PYTHON) $< --config config.ini &> $@

$(OUTFILES) : %.out : %.py
	$(PYTHON) $< > $@

cleanfiles:
	rm -f *.db
	rm -f *.h5

clean:
	rm -f *.db
	rm -f *.h5
	rm -f python-logo.png
	rm -f *.pyc
	rm -f *.out
	rm -f *.png
	rm -f *.jpg
	rm -f *.svg
	rm -f *.plot
	rm -f plots/plot2*.png
	rm -f plots/plot2_canned_report.html
	rm -f plots/dk_xplore_*.png
