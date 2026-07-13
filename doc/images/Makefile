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
				InferSchema.svg
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
OPTIONS   = 	-A -s0 -m n -o dot
OPTSINGLE = 	-A -s0 -a1 -m n -o dot

# graphviz rendering options used by the %.svg: %.dot pattern rule below.
# emitting dot from pyreverse (which is native) avoids the "format svg is not
# supported natively" notice and lets us control layout for legibility:
# larger helvetica fonts, wider node/rank spacing and a transparent
# background that blends into both light and dark documents.
DOT       = 	dot
DOTFLAGS  = 	-Tsvg \
				-Gfontname=Helvetica -Gbgcolor=transparent \
				-Gnodesep=0.4 -Granksep=0.6 \
				-Nfontname=Helvetica -Nfontsize=11 \
				-Efontname=Helvetica -Efontsize=10

all:		$(OUTFILES)

# render any generated dot file to svg with consistent, legible styling
%.svg: %.dot
	$(DOT) $(DOTFLAGS) $< -o $@

# multiprocessing
classes_multiprocessing.dot: $(CLASSROOT)/multi_processing.py
	$(PYREVERSE) -p multiprocessing -k -o dot $(CLASSROOT)/multi_processing.py

ListPipeline.dot: $(CLASSROOT)/multi_processing.py
		$(PYREVERSE) $(OPTSINGLE) -c ListPipeline $(CLASSROOT)/multi_processing.py

TaskPipeline.dot: $(CLASSROOT)/multi_processing.py
		$(PYREVERSE) $(OPTSINGLE) -c TaskPipeline $(CLASSROOT)/multi_processing.py

ListMessage.dot: $(CLASSROOT)/multi_processing.py
		$(PYREVERSE) $(OPTSINGLE) -c ListMessage $(CLASSROOT)/multi_processing.py

MD5TaskMessage.dot: $(CLASSROOT)/multi_processing.py
		$(PYREVERSE) $(OPTSINGLE) -c MD5TaskMessage $(CLASSROOT)/multi_processing.py

UIDTaskMessage.dot: $(CLASSROOT)/multi_processing.py
		$(PYREVERSE) $(OPTSINGLE) -c UIDTaskMessage $(CLASSROOT)/multi_processing.py

Journal.dot: $(CLASSROOT)/multi_processing.py
		$(PYREVERSE) $(OPTSINGLE) -c Journal $(CLASSROOT)/multi_processing.py

Worker.dot: $(CLASSROOT)/multi_processing.py
		$(PYREVERSE) $(OPTSINGLE) -c Worker $(CLASSROOT)/multi_processing.py


# Algorithms
TDigest.dot: $(CLASSROOT)/algorithms/tdigest.py
		$(PYREVERSE) $(OPTSINGLE) -c TDigest $(CLASSROOT)/algorithms/tdigest.py

# CORE
# base.py
ArgumentsMixin.dot: $(CLASSROOT)/base.py
		$(PYREVERSE) $(OPTSINGLE) -c ArgumentsMixin $(CLASSROOT)/base.py

ConfigMixin.dot: $(CLASSROOT)/base.py
		$(PYREVERSE) $(OPTSINGLE) -c ConfigMixin $(CLASSROOT)/base.py

ConfiguredApplication.dot: $(CLASSROOT)/base.py
		$(PYREVERSE) $(OPTSINGLE) -c ConfiguredApplication $(CLASSROOT)/base.py

ConfiguredObject.dot: $(CLASSROOT)/base.py
		$(PYREVERSE) $(OPTSINGLE) -c ConfiguredObject $(CLASSROOT)/base.py

Repository.dot: $(CLASSROOT)/base.py
		$(PYREVERSE) $(OPTSINGLE) -c Repository $(CLASSROOT)/base.py

InitArgumentsMixin.dot: $(CLASSROOT)/base.py
		$(PYREVERSE) $(OPTSINGLE) -c InitArgumentsMixin $(CLASSROOT)/base.py

InitConfigMixin.dot: $(CLASSROOT)/base.py
		$(PYREVERSE) $(OPTSINGLE) -c InitConfigMixin $(CLASSROOT)/base.py

ConsoleApplication.dot: $(CLASSROOT)/base.py
		$(PYREVERSE) $(OPTSINGLE) -c ConsoleApplication $(CLASSROOT)/base.py

#
# Data
#

classes_aggregation.dot: $(CLASSROOT)/data/aggregation.py
	pyreverse -o dot -ASmy -k -p aggregation dkit.data.aggregation

GroupBy.dot: $(CLASSROOT)/data/aggregation.py
		$(PYREVERSE) $(OPTSINGLE) -c GroupBy $(CLASSROOT)/data/aggregation.py

Count.dot: $(CLASSROOT)/data/aggregation.py
		$(PYREVERSE) $(OPTSINGLE) -c Count $(CLASSROOT)/data/aggregation.py

Aggregate.dot: $(CLASSROOT)/data/aggregation.py
		$(PYREVERSE) $(OPTSINGLE) -c Aggregate $(CLASSROOT)/data/aggregation.py

Bin.dot: $(CLASSROOT)/data/histogram.py
		$(PYREVERSE) $(OPTSINGLE) -c Bin  $(CLASSROOT)/data/histogram.py

Histogram.dot: $(CLASSROOT)/data/histogram.py
		$(PYREVERSE) $(OPTSINGLE) -c Histogram  $(CLASSROOT)/data/histogram.py

## containers
DictionaryEmulator.dot: $(CLASSROOT)/data/containers.py
		$(PYREVERSE) $(OPTSINGLE) -c DictionaryEmulator $(CLASSROOT)/data/containers.py

SortedCollection.dot: $(CLASSROOT)/data/containers.py
		$(PYREVERSE) $(OPTSINGLE) -c SortedCollection $(CLASSROOT)/data/containers.py

_Shelve.dot: $(CLASSROOT)/data/containers.py
		$(PYREVERSE) $(OPTSINGLE) -c _Shelve $(CLASSROOT)/data/containers.py

FastFlexShelve.dot: $(CLASSROOT)/data/containers.py
		$(PYREVERSE) $(OPTSINGLE) -c FastFlexShelve $(CLASSROOT)/data/containers.py

FlexShelve.dot: $(CLASSROOT)/data/containers.py
		$(PYREVERSE) $(OPTSINGLE) -c FlexShelve $(CLASSROOT)/data/containers.py

FlexBSDDBShelve.dot: $(CLASSROOT)/data/containers.py
		$(PYREVERSE) $(OPTSINGLE) -c FlexBSDDBShelve $(CLASSROOT)/data/containers.py

## filter.Proxy
Proxy.dot: $(CLASSROOT)/data/filters.py
		$(PYREVERSE) $(OPTSINGLE) -c Proxy $(CLASSROOT)/data/filters.py

## manipulate.py
AttrDict.dot: $(CLASSROOT)/data/containers.py
		$(PYREVERSE) $(OPTSINGLE) -c AttrDict $(CLASSROOT)/data/containers.py

Substitute.dot: $(CLASSROOT)/data/manipulate.py
		$(PYREVERSE) $(OPTSINGLE) -c Substitute $(CLASSROOT)/data/manipulate.py

OrderedSet.dot: $(CLASSROOT)/data/containers.py
		$(PYREVERSE) $(OPTIONS) -c OrderedSet $(CLASSROOT)/data/containers.py

InferSchema.dot: $(CLASSROOT)/data/infer.py
		$(PYREVERSE) $(OPTIONS) -c InferSchema $(CLASSROOT)/data/infer.py

Pivot.dot: $(CLASSROOT)/data/manipulate.py
		$(PYREVERSE) $(OPTSINGLE) -c Pivot $(CLASSROOT)/data/manipulate.py

ReducePivot.dot: $(CLASSROOT)/data/manipulate.py
		$(PYREVERSE) $(OPTSINGLE) -c ReducePivot $(CLASSROOT)/data/manipulate.py

Accumulator.dot: $(CLASSROOT)/data/stats.py
		$(PYREVERSE) $(OPTSINGLE) -c  Accumulator $(CLASSROOT)/data/stats.py

XmlTransformer.dot: $(CLASSROOT)/data/xml_helper.py
		$(PYREVERSE) $(OPTSINGLE) -c XmlTransformer $(CLASSROOT)/data/xml_helper.py

# map_db
Object.dot: $(CLASSROOT)/data/map_db.py
		$(PYREVERSE) $(OPTSINGLE) -c Object $(CLASSROOT)/data/map_db.py

ObjectMap.dot: $(CLASSROOT)/data/map_db.py
		$(PYREVERSE) $(OPTSINGLE) -c ObjectMap $(CLASSROOT)/data/map_db.py

ObjectMapDB.dot: $(CLASSROOT)/data/map_db.py
		$(PYREVERSE) $(OPTSINGLE) -c ObjectMapDB $(CLASSROOT)/data/map_db.py

FileObjectMapDB.dot: $(CLASSROOT)/data/map_db.py
		$(PYREVERSE) $(OPTSINGLE) -c FileObjectMapDB $(CLASSROOT)/data/map_db.py

FileLoaderMixin.dot: $(CLASSROOT)/data/map_db.py
		$(PYREVERSE) $(OPTSINGLE) -c FileLoaderMixin $(CLASSROOT)/data/map_db.py

# matching
FieldSpec.dot: $(CLASSROOT)/data/matching.py
		$(PYREVERSE) $(OPTSINGLE) -c FieldSpec $(CLASSROOT)/data/matching.py

DictMatcher.dot: $(CLASSROOT)/data/matching.py
		$(PYREVERSE) $(OPTIONS) -c DictMatcher $(CLASSROOT)/data/matching.py
#
#
# Parsers
#
InfixParser.dot: $(CLASSROOT)/parsers/infix_parser.py
		$(PYREVERSE) $(OPTSINGLE) -c InfixParser $(CLASSROOT)/parsers/infix_parser.py

HTMLTableParser.dot: $(CLASSROOT)/parsers/html_parser.py
		$(PYREVERSE) $(OPTSINGLE) -c HTMLTableParser $(CLASSROOT)/parsers/html_parser.py

MatchScanner.dot: $(CLASSROOT)/parsers/helpers.py
		$(PYREVERSE) $(OPTSINGLE) -c MatchScanner $(CLASSROOT)/parsers/helpers.py

SearchScanner.dot: $(CLASSROOT)/parsers/helpers.py
		$(PYREVERSE) $(OPTSINGLE) -c SearchScanner $(CLASSROOT)/parsers/helpers.py

TypeParser.dot: $(CLASSROOT)/parsers/type_parser.py
		$(PYREVERSE) $(OPTSINGLE) -c TypeParser $(CLASSROOT)/parsers/type_parser.py

URIStruct.dot: $(CLASSROOT)/parsers/uri_parser.py
		$(PYREVERSE) $(OPTSINGLE) -c URIStruct $(CLASSROOT)/parsers/uri_parser.py

#
# Plot
#
classes_plot.dot: $(CLASSROOT)/plot/*.py
	pyreverse -o dot -ASmy -k -p plot dkit.plot

Aesthetic.dot: $(CLASSROOT)/plot/ggrammar.py
		$(PYREVERSE) $(OPTSINGLE) -c Aesthetic $(CLASSROOT)/plot/ggrammar.py

XAxis.dot: $(CLASSROOT)/plot/ggrammar.py
		$(PYREVERSE) $(OPTSINGLE) -c XAxis $(CLASSROOT)/plot/ggrammar.py

YAxis.dot: $(CLASSROOT)/plot/ggrammar.py
		$(PYREVERSE) $(OPTSINGLE) -c YAxis $(CLASSROOT)/plot/ggrammar.py

Title.dot: $(CLASSROOT)/plot/ggrammar.py
		$(PYREVERSE) $(OPTSINGLE) -c Title $(CLASSROOT)/plot/ggrammar.py

GeomHistogram.dot: $(CLASSROOT)/plot/ggrammar.py
		$(PYREVERSE) $(OPTSINGLE) -c GeomHistogram $(CLASSROOT)/plot/ggrammar.py

Plot.dot: $(CLASSROOT)/plot/ggrammar.py
		$(PYREVERSE) $(OPTSINGLE) -c Plot $(CLASSROOT)/plot/ggrammar.py

AbstractGeom.dot: $(CLASSROOT)/plot/ggrammar.py
		$(PYREVERSE) $(OPTSINGLE) -c AbstractGeom $(CLASSROOT)/plot/ggrammar.py

GeomBar.dot: $(CLASSROOT)/plot/ggrammar.py
		$(PYREVERSE) $(OPTSINGLE) -c GeomBar $(CLASSROOT)/plot/ggrammar.py

GeomLine.dot: $(CLASSROOT)/plot/ggrammar.py Makefile
		$(PYREVERSE) $(OPTSINGLE) -c GeomLine $(CLASSROOT)/plot/ggrammar.py

GeomScatter.dot: $(CLASSROOT)/plot/ggrammar.py
		$(PYREVERSE) $(OPTSINGLE) -c GeomScatter $(CLASSROOT)/plot/ggrammar.py

GeomArea.dot: $(CLASSROOT)/plot/ggrammar.py
		$(PYREVERSE) $(OPTSINGLE) -c GeomArea $(CLASSROOT)/plot/ggrammar.py


BackendGnuPlot.dot: $(CLASSROOT)/plot/gnuplot.py
		$(PYREVERSE) $(OPTSINGLE) -c BackendGnuPlot $(CLASSROOT)/plot/gnuplot.py
#
# ETL
#
$(ETLSOURCE:.svg=.dot): $(CLASSROOT)/etl/source.py
		$(PYREVERSE) $(OPTIONS) -c $(patsubst %.dot,%,$@) $(CLASSROOT)/etl/source.py

ETLServices.dot: $(CLASSROOT)/etl/model.py
		$(PYREVERSE) $(OPTSINGLE) -c ETLServices $(CLASSROOT)/etl/model.py

Relation.dot: $(CLASSROOT)/etl/model.py
		$(PYREVERSE) $(OPTSINGLE) -c Relation $(CLASSROOT)/etl/model.py

Entity.dot: $(CLASSROOT)/etl/model.py
		$(PYREVERSE) $(OPTSINGLE) -c Entity $(CLASSROOT)/etl/model.py

ModelManager.dot: $(CLASSROOT)/etl/model.py
		$(PYREVERSE) $(OPTSINGLE) -c ModelManager $(CLASSROOT)/etl/model.py

Query.dot: $(CLASSROOT)/etl/model.py
		$(PYREVERSE) $(OPTSINGLE) -c Query $(CLASSROOT)/etl/model.py

Endpoint.dot: $(CLASSROOT)/etl/model.py
		$(PYREVERSE) $(OPTSINGLE) -c Endpoint $(CLASSROOT)/etl/model.py

ShelveVerifier.dot: $(CLASSROOT)/etl/verifier.py
		$(PYREVERSE) $(OPTSINGLE) -c ShelveVerifier $(CLASSROOT)/etl/verifier.py

Connection.dot: $(CLASSROOT)/etl/model.py
		$(PYREVERSE) $(OPTSINGLE) -c Connection $(CLASSROOT)/etl/model.py

Transform.dot: $(CLASSROOT)/etl/model.py
		$(PYREVERSE) $(OPTSINGLE) -c Transform $(CLASSROOT)/etl/model.py

$(SQL_ALCHEMY_SOURCE:.svg=.dot): $(CLASSROOT)/etl/extensions/ext_sql_alchemy.py
		$(PYREVERSE) $(OPTIONS) -c $(patsubst %.dot,%,$@) $(CLASSROOT)/etl/extensions/ext_sql_alchemy.py

$(PYTABLES_SOURCE:.svg=.dot): $(CLASSROOT)/etl/extensions/ext_tables.py
		$(PYREVERSE) $(OPTIONS) -c $(patsubst %.dot,%,$@) $(CLASSROOT)/etl/extensions/ext_tables.py

XmlSource.dot: $(CLASSROOT)/etl/extensions/ext_xml.py
		$(PYREVERSE) $(OPTIONS) -c  XmlSource $(CLASSROOT)/etl/extensions/ext_xml.py

EntityValidator.dot: $(CLASSROOT)/etl/schema.py
		$(PYREVERSE) $(OPTIONS) -c EntityValidator $(CLASSROOT)/etl/schema.py

ExpressionFilter.dot: $(CLASSROOT)/data/filters.py
		$(PYREVERSE) -A -mn -f ALL -c ExpressionFilter -a2 -o dot $(CLASSROOT)/data/filters.py

#
# Utilities
#
Pie.dot: $(CLASSROOT)/utilities/security.py
		$(PYREVERSE) $(OPTSINGLE) -c Pie $(CLASSROOT)/utilities/security.py

Fernet.dot: $(CLASSROOT)/utilities/security.py
		$(PYREVERSE) $(OPTSINGLE) -c Fernet $(CLASSROOT)/utilities/security.py

Vigenere.dot: $(CLASSROOT)/utilities/security.py
		$(PYREVERSE) $(OPTSINGLE) -c Vigenere $(CLASSROOT)/utilities/security.py

Timer.dot:  $(CLASSROOT)/utilities/instrumentation.py
		$(PYREVERSE) $(OPTSINGLE) -c Timer $(CLASSROOT)/utilities/instrumentation.py

Counter.dot: $(CLASSROOT)/utilities/instrumentation.py
		$(PYREVERSE) $(OPTSINGLE) -c Counter $(CLASSROOT)/utilities/instrumentation.py

CounterLogger.dot: $(CLASSROOT)/utilities/instrumentation.py
		$(PYREVERSE) $(OPTSINGLE) -c CounterLogger $(CLASSROOT)/utilities/instrumentation.py

ClassDocumenter.dot: $(CLASSROOT)/utilities/introspection.py
		$(PYREVERSE) $(OPTSINGLE) -c ClassDocumenter $(CLASSROOT)/utilities/introspection.py

ModuleDocumenter.dot: $(CLASSROOT)/utilities/introspection.py
		$(PYREVERSE) $(OPTSINGLE) -c ModuleDocumenter $(CLASSROOT)/utilities/introspection.py

FunctionDocumenter.dot: $(CLASSROOT)/utilities/introspection.py
		$(PYREVERSE) $(OPTSINGLE) -c FunctionDocumenter $(CLASSROOT)/utilities/introspection.py

# keep the intermediate dot files so make does not regenerate svgs every run
.SECONDARY:

clean:
	rm -f *.svg
	rm -f *.dot
	rm -f *.pyc
