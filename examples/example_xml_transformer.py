from dkit.data.xml_helper import XmlTransformer

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

transformer = XmlTransformer("row", schema)
for row in transformer.iter_string(xml):
    print(row)
