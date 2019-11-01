# Copyright (c) 2019 Cobus Nel
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
from .. import model
import importlib
from typing import Dict


def create_erd(entities: Dict[str, model.Entity], relations: Dict[str, model.Relation]) -> str:
    """
    Create ERD in graphviz dot format

    Arguments:
        - entities: dictionary of entities
        - relations: dictionary of relations

    """
    str_template = """digraph ERD {
graph [fontname="Arial", rankdir="LR"];
ranksep=2;
nodesep=1;
size="8.3,11.7!"

{% for entity_name, entity in entities.items() %}
"{{ entity_name }}" [shape=none, margin=0, label=<
<TABLE sides="tlrb" border="0" CELLBORDER="1" CELLSPACING="0" CELLPADDING="0" width="200">
    <TR><TD>{{ entity_name }}</TD></TR>
    {%- for column in entity.sorted_dict.keys() %}
    {%- if loop.last %}
    <TR><TD  height="0.1" sides="lrb" align="left" port="{{ column }}">{{ column }}</TD></TR>
    {%- else %}
    <TR><TD  height="0.15" sides="lr" align="left" port="{{ column }}">{{ column }}</TD></TR>
    {%- endif %}
    {%- endfor %}
</TABLE>
>];
{% endfor %}

# Relationships

{%- for relation in relations.values() %}
{%- for i in range(len(relation.constrained_columns)) %}
"{{ relation.referred_entity }}":"{{ relation.referred_columns[i] }}" -> "{{ relation.constrained_entity }}":"{{ relation.constrained_columns[i] }}"  [arrowhead = inv];
{%- endfor %}
{%- endfor %}
}
    """
    jinja2 = importlib.import_module("jinja2")
    template = jinja2.Template(str_template)
    return template.render(
        entities=entities,
        relations=relations,
        len=len
    )
