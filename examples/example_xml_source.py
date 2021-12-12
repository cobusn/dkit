from dkit.etl import reader
from dkit.etl.extensions import ext_xml

xml = """
<rows>
<row id="1" qty="3"><name>John</name></row>
<row id="2" qty="1"><name>Sam</name></row>
<row id="3" qty="5"><name>Angie</name></row>
</rows>
"""

schema = {
    "id": 'xpath("./@id")',
    "name": 'xpath("./name/text()")',
    "cost": '${quantity} * 5.0',
    "quantity": 'float(xpath("./@qty"))',
}

src = ext_xml.XmlSource(
    [reader.BytesStringReader(xml)],
    boundary="row",
    fields_dict=schema
)

for row in src:
    print(row)
