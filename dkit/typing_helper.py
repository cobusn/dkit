from typing import Any, TypeAlias, Union
from collections.abc import Iterable, Mapping
from datetime import date, datetime

AnyDate = Union[date, datetime]
Row: TypeAlias = Mapping[str, Any]
RowIterable: TypeAlias = Iterable[Row]
FieldDefinition: TypeAlias = Mapping[str, Any]
