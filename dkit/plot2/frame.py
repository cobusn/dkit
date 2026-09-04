# Copyright (c) 2026 Cobus Nel
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
"""
Row data access for dkit.plot2

plot2 consumes *rows*: an iterable of mappings, with fields referenced by
name.  :class:`~dkit.plot2.frame.Frame` is the thin wrapper that every layer reads through.  It
exists for three reasons:

* the input is materialised exactly **once**, so a generator is not consumed
  by the first layer and found empty by the second,
* filtering is defined in one place (``where=`` expressions), rather than
  duplicated in a base class and a backend,
* an omitted ``x`` field falls back to the implicit row index, without
  injecting a magic ``_index`` key into the caller's own data.
"""
from functools import lru_cache
from typing import Any, Callable, Iterable, Mapping, Union

from ..data.filters import ExpressionFilter
from ..exceptions import DKitPlotException


__all__ = ["Frame"]

#: value that means "use the implicit row index"
INDEX = None


@lru_cache(maxsize=128)
def _compiled(expression: str) -> Callable:
    """compiled ``where`` expression

    Parsing an expression is not free and the same predicate is typically
    applied to several plots, so compiled filters are cached.
    """
    try:
        return ExpressionFilter(expression)
    except Exception as e:
        raise DKitPlotException(f"invalid where expression {expression!r}: {e}")


class Frame:
    """materialised rows, with named field access

    args:
        data: an iterable of mappings, or another Frame
        where: optional filter expression, e.g. ``"${sales} > 100"``

    raises:
        DKitPlotException: if a row is not a mapping
    """
    __slots__ = ("rows",)

    def __init__(self, data: Union["Frame", Iterable[Mapping]], where: Union[str, None] = None):
        if isinstance(data, Frame):
            rows = data.rows
        else:
            rows = list(data)
            if rows and not isinstance(rows[0], Mapping):
                raise DKitPlotException(
                    "plot2 requires rows: an iterable of mappings, not "
                    f"{type(rows[0]).__name__}"
                )
        self.rows: list[Mapping] = self._apply(rows, where)

    @staticmethod
    def _apply(rows: list, where: Union[str, None]) -> list:
        if not where:
            return rows
        predicate = _compiled(where)
        return [r for r in rows if predicate(r)]

    def __len__(self) -> int:
        return len(self.rows)

    def __iter__(self):
        return iter(self.rows)

    def __getitem__(self, index):
        return self.rows[index]

    def __repr__(self) -> str:
        return f"Frame({len(self.rows)} rows, fields={self.fields})"

    @property
    def fields(self) -> list[str]:
        """field names, in the order the first row defines them"""
        if not self.rows:
            return []
        return list(self.rows[0].keys())

    def filter(self, where: Union[str, None]) -> "Frame":
        """a new Frame holding only the rows matching ``where``

        Returns ``self`` when ``where`` is empty, so the common unfiltered
        case copies nothing.
        """
        if not where:
            return self
        return Frame(self, where)

    def values(self, field: Union[str, None] = INDEX) -> list:
        """values of ``field``, or the implicit row index if ``field`` is None

        args:
            field: field name, or None for ``[0, 1, 2, ...]``

        raises:
            DKitPlotException: if ``field`` is missing from a row
        """
        if field is INDEX:
            return list(range(len(self.rows)))
        try:
            return [r[field] for r in self.rows]
        except KeyError:
            raise DKitPlotException(
                f"field {field!r} not in data. available fields: "
                f"{', '.join(self.fields) or 'none'}"
            )

    def distinct(self, field: Union[str, None] = INDEX) -> list:
        """distinct values of ``field``, in order of first appearance

        Order matters: it becomes the tick order of a categorical scale, so it
        must not depend on set iteration order.
        """
        return list(dict.fromkeys(self.values(field)))

    def groups(self, field: str) -> dict[Any, "Frame"]:
        """rows split by the value of ``field``, in order of first appearance"""
        rv: dict[Any, list] = {}
        for row in self.rows:
            try:
                key = row[field]
            except KeyError:
                raise DKitPlotException(
                    f"field {field!r} not in data. available fields: "
                    f"{', '.join(self.fields) or 'none'}"
                )
            rv.setdefault(key, []).append(row)
        return {k: Frame(v) for k, v in rv.items()}
