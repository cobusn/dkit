# 
# Copyright (C) 2014  Cobus Nel
#
# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 2.1 of the License, or (at your option) any later version.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# License along with this library; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
#
ALGORITHMS =    TDigest.svg
CORE      =     Repository.svg ArgumentsMixin.svg\
                InitArgumentsMixin.svg ConfigMixin.svg InitConfigMixin.svg ConfiguredObject.svg\
                ConfiguredApplication.svg ConsoleApplication.svg 
UTILITIES =     Counter.svg CounterLogger.svg Timer.svg ClassDocumenter.svg ModuleDocumenter.svg \
				FunctionDocumenter.svg Vigenere.svg Fernet.svg Pie.svg
DATA      =     AttrDict.svg Pivot.svg ReducePivot.svg Proxy.svg XmlTransformer.svg\
				ObjectMap.svg ObjectMapDB.svg FileObjectMapDB.svg FileLoaderMixin.svg\
				DictionaryEmulator.svg Accumulator.svg FieldSpec.svg DictMatcher.svg\
				Object.svg SortedCollection.svg Histogram.svg Bin.svg classes_aggregation.svg \
				Aggregate.svg GroupBy.svg Count.svg _Shelve.svg FlexBSDDBShelve.svg \
				FlexShelve.svg FastFlexShelve.svg Substitute.svg OrderedSet.svg \
				InferTypes.svg
ETLSOURCE =     AbstractSource.svg FileListingSource.svg AbstractMultiReaderSource.svg PickleSource.svg\
				JsonlSource.svg CsvDictSource.svg XmlRpcSource.svg
ETLCLASSES =	EntityValidator.svg Entity.svg Transform.svg ModelManager.svg ExpressionFilter.svg \
				Connection.svg Query.svg Endpoint.svg ShelveVerifier.svg Relation.svg ETLServices.svg  \
				XmlSource.svg
SQL_ALCHEMY_SOURCE = SQLAlchemyAccessor.svg SQLAlchemyTableSource.svg SQLAlchemySink.svg \
					 SQLAlchemyModelFactory.svg SQLAlchemyReflector.svg SQLAlchemySelectSource.svg \
					SQLAlchemyTemplateSource.svg
PYTABLES_SOURCE = PyTablesAccessor.svg PyTablesSource.svg PyTablesSink.svg PyTablesModelFactory.svg \
				  PyTablesReflector.svg PyTablesServices.svg
ETL       = 	$(ETLSOURCE) $(SQL_ALCHEMY_SOURCE) $(PYTABLES_SOURCE) $(ETLCLASSES) 
PARSERS   = 	URIStruct.svg SearchScanner.svg MatchScanner.svg InfixParser.svg HTMLTableParser.svg\
				TypeParser.svg
PLOT      =     classes_plot.svg Plot.svg GeomLine.svg GeomArea.svg GeomScatter.svg GeomBar.svg AbstractGeom.svg\
				BackendGnuPlot.svg Aesthetic.svg YAxis.svg XAxis.svg Title.svg GeomHistogram.svg

MULTIPROCESSING = classes_multiprocessing.svg ListPipeline.svg TaskPipeline.svg ListMessage.svg \
				  MD5TaskMessage.svg UIDTaskMessage.svg Journal.svg Worker.svg


OUTFILES  = 	Makefile $(ALGORITHMS) $(UTILITIES) $(DATA) $(CORE) $(SECURITY) $(ETL) \
				$(PARSERS) $(PLOT) $(MULTIPROCESSING)

CLASSROOT = 	../../dkit
PYREVERSE = 	pyreverse
OPTIONS   = 	-A -s0 -m n -o svg
OPTSINGLE = 	-A -s0 -a1 -m n -o svg

all:		$(OUTFILES) 

# multiprocessing
classes_multiprocessing.svg: $(CLASSROOT)/multi_processing.py
	$(PYREVERSE) -p multiprocessing -k -o svg $(CLASSROOT)/multi_processing.py

ListPipeline.svg: $(CLASSROOT)/multi_processing.py
		$(PYREVERSE) $(OPTSINGLE) -c ListPipeline $(CLASSROOT)/multi_processing.py

TaskPipeline.svg: $(CLASSROOT)/multi_processing.py
		$(PYREVERSE) $(OPTSINGLE) -c TaskPipeline $(CLASSROOT)/multi_processing.py

ListMessage.svg: $(CLASSROOT)/multi_processing.py
		$(PYREVERSE) $(OPTSINGLE) -c ListMessage $(CLASSROOT)/multi_processing.py

MD5TaskMessage.svg: $(CLASSROOT)/multi_processing.py
		$(PYREVERSE) $(OPTSINGLE) -c MD5TaskMessage $(CLASSROOT)/multi_processing.py

UIDTaskMessage.svg: $(CLASSROOT)/multi_processing.py
		$(PYREVERSE) $(OPTSINGLE) -c UIDTaskMessage $(CLASSROOT)/multi_processing.py

Journal.svg: $(CLASSROOT)/multi_processing.py
		$(PYREVERSE) $(OPTSINGLE) -c Journal $(CLASSROOT)/multi_processing.py

Worker.svg: $(CLASSROOT)/multi_processing.py
		$(PYREVERSE) $(OPTSINGLE) -c Worker $(CLASSROOT)/multi_processing.py


# Algorithms
TDigest.svg: $(CLASSROOT)/algorithms/tdigest.py 
		$(PYREVERSE) $(OPTSINGLE) -c TDigest $(CLASSROOT)/algorithms/tdigest.py

# CORE
# base.py
ArgumentsMixin.svg: $(CLASSROOT)/base.py 
		$(PYREVERSE) $(OPTSINGLE) -c ArgumentsMixin $(CLASSROOT)/base.py

ConfigMixin.svg: $(CLASSROOT)/base.py 
		$(PYREVERSE) $(OPTSINGLE) -c ConfigMixin $(CLASSROOT)/base.py

ConfiguredApplication.svg: $(CLASSROOT)/base.py 
		$(PYREVERSE) $(OPTSINGLE) -c ConfiguredApplication $(CLASSROOT)/base.py

ConfiguredObject.svg: $(CLASSROOT)/base.py 
		$(PYREVERSE) $(OPTSINGLE) -c ConfiguredObject $(CLASSROOT)/base.py

Repository.svg: $(CLASSROOT)/base.py 
		$(PYREVERSE) $(OPTSINGLE) -c Repository $(CLASSROOT)/base.py

InitArgumentsMixin.svg: $(CLASSROOT)/base.py 
		$(PYREVERSE) $(OPTSINGLE) -c InitArgumentsMixin $(CLASSROOT)/base.py

InitConfigMixin.svg: $(CLASSROOT)/base.py 
		$(PYREVERSE) $(OPTSINGLE) -c InitConfigMixin $(CLASSROOT)/base.py

ConsoleApplication.svg: $(CLASSROOT)/base.py 
		$(PYREVERSE) $(OPTSINGLE) -c ConsoleApplication $(CLASSROOT)/base.py

# 
# Data
#

classes_aggregation.svg: $(CLASSROOT)/data/aggregation.py 
	pyreverse -o svg -ASmy -k -p aggregation dkit.data.aggregation

GroupBy.svg: $(CLASSROOT)/data/aggregation.py 
		$(PYREVERSE) $(OPTSINGLE) -c GroupBy $(CLASSROOT)/data/aggregation.py

Count.svg: $(CLASSROOT)/data/aggregation.py 
		$(PYREVERSE) $(OPTSINGLE) -c Count $(CLASSROOT)/data/aggregation.py

Aggregate.svg: $(CLASSROOT)/data/aggregation.py 
		$(PYREVERSE) $(OPTSINGLE) -c Aggregate $(CLASSROOT)/data/aggregation.py

Bin.svg: $(CLASSROOT)/data/histogram.py 
		$(PYREVERSE) $(OPTSINGLE) -c Bin  $(CLASSROOT)/data/histogram.py

Histogram.svg: $(CLASSROOT)/data/histogram.py 
		$(PYREVERSE) $(OPTSINGLE) -c Histogram  $(CLASSROOT)/data/histogram.py

## containers
DictionaryEmulator.svg: $(CLASSROOT)/data/containers.py 
		$(PYREVERSE) $(OPTSINGLE) -c DictionaryEmulator $(CLASSROOT)/data/containers.py

SortedCollection.svg: $(CLASSROOT)/data/containers.py 
		$(PYREVERSE) $(OPTSINGLE) -c SortedCollection $(CLASSROOT)/data/containers.py

_Shelve.svg: $(CLASSROOT)/data/containers.py 
		$(PYREVERSE) $(OPTSINGLE) -c _Shelve $(CLASSROOT)/data/containers.py

FastFlexShelve.svg: $(CLASSROOT)/data/containers.py 
		$(PYREVERSE) $(OPTSINGLE) -c FastFlexShelve $(CLASSROOT)/data/containers.py

FlexShelve.svg: $(CLASSROOT)/data/containers.py 
		$(PYREVERSE) $(OPTSINGLE) -c FlexShelve $(CLASSROOT)/data/containers.py

FlexBSDDBShelve.svg: $(CLASSROOT)/data/containers.py 
		$(PYREVERSE) $(OPTSINGLE) -c FlexBSDDBShelve $(CLASSROOT)/data/containers.py

## filter.Proxy
Proxy.svg: $(CLASSROOT)/data/filters.py 
		$(PYREVERSE) $(OPTSINGLE) -c Proxy $(CLASSROOT)/data/filters.py

## manipulate.py
AttrDict.svg: $(CLASSROOT)/data/containers.py 
		$(PYREVERSE) $(OPTSINGLE) -c AttrDict $(CLASSROOT)/data/containers.py

Substitute.svg: $(CLASSROOT)/data/manipulate.py 
		$(PYREVERSE) $(OPTSINGLE) -c Substitute $(CLASSROOT)/data/manipulate.py

OrderedSet.svg: $(CLASSROOT)/data/containers.py 
		$(PYREVERSE) $(OPTIONS) -c OrderedSet $(CLASSROOT)/data/containers.py

InferTypes.svg: $(CLASSROOT)/data/infer.py 
		$(PYREVERSE) $(OPTIONS) -c InferTypes $(CLASSROOT)/data/infer.py

Pivot.svg: $(CLASSROOT)/data/manipulate.py 
		$(PYREVERSE) $(OPTSINGLE) -c Pivot $(CLASSROOT)/data/manipulate.py

ReducePivot.svg: $(CLASSROOT)/data/manipulate.py 
		$(PYREVERSE) $(OPTSINGLE) -c ReducePivot $(CLASSROOT)/data/manipulate.py

Accumulator.svg: $(CLASSROOT)/data/stats.py 
		$(PYREVERSE) $(OPTSINGLE) -c  Accumulator $(CLASSROOT)/data/stats.py

XmlTransformer.svg: $(CLASSROOT)/data/xml_helper.py 
		$(PYREVERSE) $(OPTSINGLE) -c XmlTransformer $(CLASSROOT)/data/xml_helper.py

# map_db
Object.svg: $(CLASSROOT)/data/map_db.py 
		$(PYREVERSE) $(OPTSINGLE) -c Object $(CLASSROOT)/data/map_db.py

ObjectMap.svg: $(CLASSROOT)/data/map_db.py 
		$(PYREVERSE) $(OPTSINGLE) -c ObjectMap $(CLASSROOT)/data/map_db.py

ObjectMapDB.svg: $(CLASSROOT)/data/map_db.py 
		$(PYREVERSE) $(OPTSINGLE) -c ObjectMapDB $(CLASSROOT)/data/map_db.py

FileObjectMapDB.svg: $(CLASSROOT)/data/map_db.py 
		$(PYREVERSE) $(OPTSINGLE) -c FileObjectMapDB $(CLASSROOT)/data/map_db.py

FileLoaderMixin.svg: $(CLASSROOT)/data/map_db.py 
		$(PYREVERSE) $(OPTSINGLE) -c FileLoaderMixin $(CLASSROOT)/data/map_db.py

# matching
FieldSpec.svg: $(CLASSROOT)/data/matching.py 
		$(PYREVERSE) $(OPTSINGLE) -c FieldSpec $(CLASSROOT)/data/matching.py

DictMatcher.svg: $(CLASSROOT)/data/matching.py 
		$(PYREVERSE) $(OPTIONS) -c DictMatcher $(CLASSROOT)/data/matching.py
#
#
# Parsers
#
InfixParser.svg: $(CLASSROOT)/parsers/infix_parser.py 
		$(PYREVERSE) $(OPTSINGLE) -c InfixParser $(CLASSROOT)/parsers/infix_parser.py

HTMLTableParser.svg: $(CLASSROOT)/parsers/html_parser.py 
		$(PYREVERSE) $(OPTSINGLE) -c HTMLTableParser $(CLASSROOT)/parsers/html_parser.py

MatchScanner.svg: $(CLASSROOT)/parsers/helpers.py 
		$(PYREVERSE) $(OPTSINGLE) -c MatchScanner $(CLASSROOT)/parsers/helpers.py

SearchScanner.svg: $(CLASSROOT)/parsers/helpers.py 
		$(PYREVERSE) $(OPTSINGLE) -c SearchScanner $(CLASSROOT)/parsers/helpers.py

TypeParser.svg: $(CLASSROOT)/parsers/type_parser.py 
		$(PYREVERSE) $(OPTSINGLE) -c TypeParser $(CLASSROOT)/parsers/type_parser.py

URIStruct.svg: $(CLASSROOT)/parsers/uri_parser.py 
		$(PYREVERSE) $(OPTSINGLE) -c URIStruct $(CLASSROOT)/parsers/uri_parser.py

#
# Plot
#
classes_plot.svg: $(CLASSROOT)/plot/*.py 
	pyreverse -o svg -ASmy -k -p plot dkit.plot

Aesthetic.svg: $(CLASSROOT)/plot/ggrammar.py 
		$(PYREVERSE) $(OPTSINGLE) -c Aesthetic $(CLASSROOT)/plot/ggrammar.py

XAxis.svg: $(CLASSROOT)/plot/ggrammar.py 
		$(PYREVERSE) $(OPTSINGLE) -c XAxis $(CLASSROOT)/plot/ggrammar.py

YAxis.svg: $(CLASSROOT)/plot/ggrammar.py 
		$(PYREVERSE) $(OPTSINGLE) -c YAxis $(CLASSROOT)/plot/ggrammar.py

Title.svg: $(CLASSROOT)/plot/ggrammar.py 
		$(PYREVERSE) $(OPTSINGLE) -c Title $(CLASSROOT)/plot/ggrammar.py

GeomHistogram.svg: $(CLASSROOT)/plot/ggrammar.py 
		$(PYREVERSE) $(OPTSINGLE) -c GeomHistogram $(CLASSROOT)/plot/ggrammar.py

Plot.svg: $(CLASSROOT)/plot/ggrammar.py 
		$(PYREVERSE) $(OPTSINGLE) -c Plot $(CLASSROOT)/plot/ggrammar.py

AbstractGeom.svg: $(CLASSROOT)/plot/ggrammar.py 
		$(PYREVERSE) $(OPTSINGLE) -c AbstractGeom $(CLASSROOT)/plot/ggrammar.py

GeomBar.svg: $(CLASSROOT)/plot/ggrammar.py 
		$(PYREVERSE) $(OPTSINGLE) -c GeomBar $(CLASSROOT)/plot/ggrammar.py

GeomLine.svg: $(CLASSROOT)/plot/ggrammar.py Makefile
		$(PYREVERSE) $(OPTSINGLE) -c GeomLine $(CLASSROOT)/plot/ggrammar.py

GeomScatter.svg: $(CLASSROOT)/plot/ggrammar.py 
		$(PYREVERSE) $(OPTSINGLE) -c GeomScatter $(CLASSROOT)/plot/ggrammar.py

GeomArea.svg: $(CLASSROOT)/plot/ggrammar.py 
		$(PYREVERSE) $(OPTSINGLE) -c GeomArea $(CLASSROOT)/plot/ggrammar.py


BackendGnuPlot.svg: $(CLASSROOT)/plot/gnuplot.py 
		$(PYREVERSE) $(OPTSINGLE) -c BackendGnuPlot $(CLASSROOT)/plot/gnuplot.py
#
# ETL
#
$(ETLSOURCE): $(CLASSROOT)/etl/source.py 
		$(PYREVERSE) $(OPTIONS) -c $(patsubst %.svg,%,$@) $(CLASSROOT)/etl/source.py

ETLServices.svg: $(CLASSROOT)/etl/model.py 
		$(PYREVERSE) $(OPTSINGLE) -c ETLServices $(CLASSROOT)/etl/model.py

Relation.svg: $(CLASSROOT)/etl/model.py 
		$(PYREVERSE) $(OPTSINGLE) -c Relation $(CLASSROOT)/etl/model.py
 
Entity.svg: $(CLASSROOT)/etl/model.py 
		$(PYREVERSE) $(OPTSINGLE) -c Entity $(CLASSROOT)/etl/model.py
 
ModelManager.svg: $(CLASSROOT)/etl/model.py 
		$(PYREVERSE) $(OPTSINGLE) -c ModelManager $(CLASSROOT)/etl/model.py

Query.svg: $(CLASSROOT)/etl/model.py 
		$(PYREVERSE) $(OPTSINGLE) -c Query $(CLASSROOT)/etl/model.py

Endpoint.svg: $(CLASSROOT)/etl/model.py 
		$(PYREVERSE) $(OPTSINGLE) -c Endpoint $(CLASSROOT)/etl/model.py

ShelveVerifier.svg: $(CLASSROOT)/etl/verifier.py 
		$(PYREVERSE) $(OPTSINGLE) -c ShelveVerifier $(CLASSROOT)/etl/verifier.py

Connection.svg: $(CLASSROOT)/etl/model.py 
		$(PYREVERSE) $(OPTSINGLE) -c Connection $(CLASSROOT)/etl/model.py

Transform.svg: $(CLASSROOT)/etl/model.py 
		$(PYREVERSE) $(OPTSINGLE) -c Transform $(CLASSROOT)/etl/model.py

$(SQL_ALCHEMY_SOURCE): $(CLASSROOT)/etl/extensions/ext_sql_alchemy.py 
		$(PYREVERSE) $(OPTIONS) -c $(patsubst %.svg,%,$@) $(CLASSROOT)/etl/extensions/ext_sql_alchemy.py

$(PYTABLES_SOURCE): $(CLASSROOT)/etl/extensions/ext_tables.py 
		$(PYREVERSE) $(OPTIONS) -c $(patsubst %.svg,%,$@) $(CLASSROOT)/etl/extensions/ext_tables.py

XmlSource.svg: $(CLASSROOT)/etl/extensions/ext_xml.py 
		$(PYREVERSE) $(OPTIONS) -c  XmlSource $(CLASSROOT)/etl/extensions/ext_xml.py

EntityValidator.svg: $(CLASSROOT)/etl/schema.py 
		$(PYREVERSE) $(OPTIONS) -c EntityValidator $(CLASSROOT)/etl/schema.py

ExpressionFilter.svg: $(CLASSROOT)/data/filters.py 
		$(PYREVERSE) -A -mn -f ALL -c ExpressionFilter -a2 -o svg $(CLASSROOT)/data/filters.py

#
# Utilities
#
Pie.svg: $(CLASSROOT)/utilities/security.py 
		$(PYREVERSE) $(OPTSINGLE) -c Pie $(CLASSROOT)/utilities/security.py

Fernet.svg: $(CLASSROOT)/utilities/security.py 
		$(PYREVERSE) $(OPTSINGLE) -c Fernet $(CLASSROOT)/utilities/security.py

Vigenere.svg: $(CLASSROOT)/utilities/security.py 
		$(PYREVERSE) $(OPTSINGLE) -c Vigenere $(CLASSROOT)/utilities/security.py

Timer.svg:  $(CLASSROOT)/utilities/instrumentation.py 
		$(PYREVERSE) $(OPTSINGLE) -c Timer $(CLASSROOT)/utilities/instrumentation.py	

Counter.svg: $(CLASSROOT)/utilities/instrumentation.py 
		$(PYREVERSE) $(OPTSINGLE) -c Counter $(CLASSROOT)/utilities/instrumentation.py

CounterLogger.svg: $(CLASSROOT)/utilities/instrumentation.py 
		$(PYREVERSE) $(OPTSINGLE) -c CounterLogger $(CLASSROOT)/utilities/instrumentation.py

ClassDocumenter.svg: $(CLASSROOT)/utilities/introspection.py 
		$(PYREVERSE) $(OPTSINGLE) -c ClassDocumenter $(CLASSROOT)/utilities/introspection.py

ModuleDocumenter.svg: $(CLASSROOT)/utilities/introspection.py 
		$(PYREVERSE) $(OPTSINGLE) -c ModuleDocumenter $(CLASSROOT)/utilities/introspection.py

FunctionDocumenter.svg: $(CLASSROOT)/utilities/introspection.py 
		$(PYREVERSE) $(OPTSINGLE) -c FunctionDocumenter $(CLASSROOT)/utilities/introspection.py

clean:
	rm -f *.svg
	rm -f *.pyc
