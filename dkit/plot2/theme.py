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
Theme support for dkit.plot2

Styling is split in two:

* **rcParams** live in native matplotlib ``.mplstyle`` files.  Anything
  matplotlib can already express is expressed there and validated by
  matplotlib itself.
* **:class:`~dkit.plot2.theme.Theme`** adds only what rcParams cannot express: sequential and
  diverging colour maps, semantic colours (positive / negative / neutral /
  highlight) and default number and date formats.

Themes are immutable and are applied *scoped*, so rendering one plot never
restyles the next::

    with theme.context():
        fig, ax = plt.subplots()
        ...

Three bundled themes are registered by name in :data:`~dkit.plot2.theme.themes`, and
:func:`~dkit.plot2.theme.get_theme` resolves whatever a caller passed -- a ``Theme``, a name, or
None -- to one of them.  A project adds its own house theme to that registry
with :func:`~dkit.plot2.theme.register_theme`, and makes it the one every unqualified plot uses
with :func:`~dkit.plot2.theme.set_default_theme`::

    register_theme("house", get_theme("dkit-light").replace(
        rc=["dkit-light", "house.mplstyle"], highlight="#c8102e",
    ))
    set_default_theme("house")

Neither call is needed to *use* a custom theme: a ``Theme`` instance is
accepted everywhere a name is.  Registering buys reaching it by name from a
config file or a command line, and a default buys not repeating ``theme=``.
"""
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field, replace as _replace
from importlib.resources import files
from os import fspath
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence, Union

import matplotlib.style as mpl_style
from matplotlib import colormaps
from matplotlib.colors import Colormap, Normalize
from matplotlib.figure import Figure

from ..exceptions import DKitPlotException


__all__ = [
    "CM_TO_INCH",
    "Theme",
    "bundled_styles",
    "default_theme",
    "get_theme",
    "register_theme",
    "resolve_style",
    "set_default_theme",
    "themes",
    "unregister_theme",
]

#: centimeters to inches, matplotlib works in inches
CM_TO_INCH = 0.393701

#: name of the package that holds bundled ``.mplstyle`` files
_STYLE_PACKAGE = "dkit.plot2.styles"

#: a style spec is anything ``matplotlib.style.use`` accepts, plus bundled names
StyleSpec = Union[str, Path, Mapping[str, Any], Sequence[Any]]


def bundled_styles() -> list[str]:
    """names of ``.mplstyle`` files bundled with dkit.plot2"""
    return sorted(
        p.name[:-len(".mplstyle")]
        for p in files(_STYLE_PACKAGE).iterdir()
        if p.name.endswith(".mplstyle")
    )


def _bundled_path(name: str) -> Union[str, None]:
    """filesystem path of a bundled style, or None if not bundled"""
    resource = files(_STYLE_PACKAGE) / f"{name}.mplstyle"
    if not resource.is_file():
        return None
    return fspath(resource)


def resolve_style(spec: StyleSpec) -> Any:
    """resolve a style spec to something ``matplotlib.style.use`` understands

    Bundled names (e.g. ``"dkit-light"``) resolve to their packaged file path.
    Anything else is passed through unchanged: built in style names, paths and
    rcParams mappings are all already understood by matplotlib.  Sequences are
    resolved element wise and applied in order, so later entries win.

    args:
        spec: bundled name, built in style name, path, rcParams mapping, or a
            sequence of any of these

    returns:
        resolved spec, ready for ``matplotlib.style.use``
    """
    if isinstance(spec, Mapping):
        return dict(spec)
    if isinstance(spec, Path):
        return fspath(spec)
    if isinstance(spec, str):
        return _bundled_path(spec) or spec
    if isinstance(spec, Iterable):
        return [resolve_style(i) for i in spec]
    raise DKitPlotException(f"cannot resolve style spec of type {type(spec).__name__}")


@dataclass(frozen=True)
class Theme:
    """look and feel for a family of plots

    args:
        rc: rcParams source.  A bundled style name, a built in matplotlib style
            name, a path to an ``.mplstyle`` file, an rcParams mapping, or a
            list of these applied in order (later wins).
        categorical: colour cycle for discrete series.  Also becomes
            ``axes.prop_cycle``, overriding whatever ``rc`` sets.
        sequential: colour map name for ordered magnitudes (heat maps)
        diverging: colour map name for values around a midpoint (deltas)
        positive: semantic colour for gains
        negative: semantic colour for losses
        neutral: semantic colour for de-emphasised marks
        highlight: semantic colour for the one series that matters.  Reserved:
            it must not be a colour the series cycle can also produce, or a
            highlighted layer is indistinguishable from whichever series draws
            in that slot.  The bundled themes keep it a magenta, which none of
            their cycles contains.
        number_format: default ``str.format`` spec for numeric tick labels, e.g.
            ``"{x:,.0f}"``.  None leaves matplotlib's own formatter in place,
            which picks a sensible number of decimals per axis; set this only
            when every numeric axis in the project should look the same.
        date_format: default ``strftime`` spec for date tick labels, e.g.
            ``"%b %Y"``.  None uses matplotlib's concise date formatter, which
            adapts to the range being plotted.
        width: figure width in centimeters.  Overrides ``figure.figsize``.
        height: figure height in centimeters.  Overrides ``figure.figsize``.
        reset: apply matplotlib defaults before ``rc``, so a theme renders the
            same regardless of ambient rcParams
    """
    rc: StyleSpec = "dkit-light"
    categorical: tuple[str, ...] = field(default=())
    sequential: str = "viridis"
    diverging: str = "RdYlGn_r"
    positive: str = "#00c389"
    negative: str = "#ef3340"
    neutral: str = "#8a8a8a"
    highlight: str = "#c8007c"
    number_format: Union[str, None] = None
    date_format: Union[str, None] = None
    width: Union[float, None] = None
    height: Union[float, None] = None
    reset: bool = True

    def replace(self, **kwargs) -> "Theme":
        """return a copy of this theme with the given fields changed"""
        return _replace(self, **kwargs)

    #
    # rcParams
    #
    def _overrides(self) -> dict:
        """rcParams derived from theme fields, applied last"""
        rv: dict = {}
        if self.categorical:
            # cycler is imported lazily: it is only needed when overriding
            from matplotlib import cycler
            rv["axes.prop_cycle"] = cycler("color", list(self.categorical))
        if self.width is not None or self.height is not None:
            rv["figure.figsize"] = self.figsize
        return rv

    def layers(self) -> list:
        """ordered list of style layers, suitable for ``matplotlib.style.use``"""
        rv: list = ["default"] if self.reset else []
        resolved = resolve_style(self.rc)
        rv.extend(resolved if isinstance(resolved, list) else [resolved])
        overrides = self._overrides()
        if overrides:
            rv.append(overrides)
        return rv

    def context(self):
        """context manager that applies this theme, then restores rcParams

        Preferred over :meth:`apply`: rendering a themed plot leaves global
        rcParams untouched, so it cannot restyle later plots.
        """
        return mpl_style.context(self.layers())

    def apply(self) -> None:
        """apply this theme to global rcParams

        Use in scripts and notebooks.  Library code should prefer
        :meth:`context`.
        """
        mpl_style.use(self.layers())

    @property
    def figsize(self) -> Union[tuple[float, float], None]:
        """(width, height) in inches, or None if neither dimension is set"""
        if self.width is None and self.height is None:
            return None
        import matplotlib as mpl
        default = mpl.rcParams["figure.figsize"]
        width = self.width * CM_TO_INCH if self.width is not None else default[0]
        height = self.height * CM_TO_INCH if self.height is not None else default[1]
        return (width, height)

    def size_figure(self, fig: Figure) -> None:
        """resize an existing figure to this theme's width and height"""
        if self.width is not None:
            fig.set_figwidth(self.width * CM_TO_INCH)
        if self.height is not None:
            fig.set_figheight(self.height * CM_TO_INCH)

    #
    # colours
    #
    def cycle(self) -> tuple[str, ...]:
        """the effective categorical cycle, falling back to ``axes.prop_cycle``"""
        if self.categorical:
            return self.categorical
        with self.context():
            import matplotlib as mpl
            return tuple(mpl.rcParams["axes.prop_cycle"].by_key()["color"])

    def color(self, index: int) -> str:
        """colour ``index`` of the categorical cycle, wrapping around"""
        cycle = self.cycle()
        return cycle[index % len(cycle)]

    def colors(self, n: int) -> list[str]:
        """first ``n`` colours of the categorical cycle, wrapping around"""
        return [self.color(i) for i in range(n)]

    def signed(self, value: float) -> str:
        """semantic colour for a signed value: positive, negative or neutral"""
        if value > 0:
            return self.positive
        elif value < 0:
            return self.negative
        return self.neutral

    def get_cmap(self, kind: str = "sequential") -> Colormap:
        """colour map by role or by name

        args:
            kind: ``"sequential"``, ``"diverging"``, or any registered
                matplotlib colour map name

        returns:
            matplotlib Colormap
        """
        name = {"sequential": self.sequential, "diverging": self.diverging}.get(kind, kind)
        try:
            return colormaps[name]
        except KeyError:
            raise DKitPlotException(f"unknown colour map: {name}")

    def map_colors(self, values: Sequence[float], kind: str = "sequential",
                   alpha: Union[float, None] = None, vmin: Union[float, None] = None,
                   vmax: Union[float, None] = None) -> list:
        """map values onto a colour map, one RGBA colour per value

        args:
            values: values to map
            kind: colour map role or name, see :meth:`get_cmap`
            alpha: alpha applied to every colour
            vmin: low end of the scale, defaults to ``min(values)``
            vmax: high end of the scale, defaults to ``max(values)``

        returns:
            list of RGBA tuples
        """
        values = list(values)
        if not values:
            return []
        cmap = self.get_cmap(kind)
        low = min(values) if vmin is None else vmin
        high = max(values) if vmax is None else vmax
        if low == high:
            # a constant series has no gradient: use the midpoint of the map
            return [cmap(0.5, alpha=alpha) for _ in values]
        norm = Normalize(vmin=low, vmax=high)
        return [cmap(norm(v), alpha=alpha) for v in values]


#: the theme registry, by name.  Bundled entries below; add to it with
#: :func:`~dkit.plot2.theme.register_theme`, which validates and refuses silent collisions.
themes: dict[str, Theme] = {
    "dkit-light": Theme(rc="dkit-light"),
    "dkit-dark": Theme(
        rc=["dkit-light", "dkit-dark"],
        sequential="mako" if "mako" in colormaps else "cividis",
        neutral="#8a8a8a",
        # a lighter magenta than the light theme's: #c8007c is too dark to read
        # against 1c1c1c.  Still absent from this theme's cycle.
        highlight="#ff4fd8",
    ),
    "dkit-console": Theme(
        rc=["dkit-light", "dkit-dark", "dkit-console"],
        categorical=("#00CC33", "#FFBF00", "#FF2B00", "#00BFFF", "#7F7F7F"),
        positive="#00CC33",
        negative="#FF2B00",
        neutral="#7F7F7F",
        highlight="#FF66CC",
    ),
    "dkit-print": Theme(rc=["dkit-light", "dkit-print"]),
}

#: theme used when no project default has been set
DEFAULT_THEME = "dkit-light"

_ACTIVE_THEME: ContextVar[Union[Theme, None]] = ContextVar(
    "dkit_plot2_active_theme", default=None
)


def current_theme() -> Union[Theme, None]:
    """Return the style-pack theme active for the current build context."""
    return _ACTIVE_THEME.get()


@contextmanager
def theme_context(theme: Theme):
    """Apply a theme and make it Plot2's implicit theme in this context.

    Args:
        theme: theme to apply and expose to plots created in the context.
    """
    token = _ACTIVE_THEME.set(theme)
    try:
        with theme.context():
            yield theme
    finally:
        _ACTIVE_THEME.reset(token)

#: the project default, replaced by :func:`~dkit.plot2.theme.set_default_theme`
_default: Union[Theme, str] = DEFAULT_THEME


def register_theme(name: str, theme: Theme, replace: bool = False) -> Theme:
    """add a theme to the :data:`~dkit.plot2.theme.themes` registry under ``name``

    Registering is only needed to reach a theme *by name* -- in a
    ``Plot(theme="house")``, a config file, or a command line argument.  A
    ``Theme`` instance is already usable everywhere a name is, so a project
    that passes its theme around explicitly never has to register it.

    args:
        name: name to register under.  Resolved before bundled style names, so
            registering ``"dkit-light"`` shadows the bundled theme of that name
            for every caller.
        theme: the Theme to register
        replace: True to overwrite an existing entry.  The default refuses,
            because a name collision is more often a typo than an intent to
            restyle the whole project.

    returns:
        the registered theme, so this can wrap a constructor expression

    raises:
        DKitPlotException: if ``theme`` is not a Theme, or the name is taken
            and ``replace`` is False
    """
    if not isinstance(theme, Theme):
        raise DKitPlotException(
            f"register_theme needs a Theme, not {type(theme).__name__}"
        )
    if name in themes and not replace:
        raise DKitPlotException(
            f"theme {name!r} is already registered; pass replace=True to override it"
        )
    themes[name] = theme
    return theme


def unregister_theme(name: str) -> Theme:
    """remove a theme from the registry and return it

    Mainly for tests, which have to leave the registry as they found it.

    raises:
        DKitPlotException: if the name is not registered, or is the current
            default -- removing that would fail later, at some unrelated
            ``render`` call, rather than here
    """
    if name not in themes:
        raise DKitPlotException(f"theme {name!r} is not registered")
    if _default == name:
        raise DKitPlotException(
            f"theme {name!r} is the current default; call set_default_theme() first"
        )
    return themes.pop(name)


def set_default_theme(spec: Union[Theme, str, None] = None) -> Union[Theme, str]:
    """set the theme used wherever none is named, and return the previous one

    This is a global mutation, and stands to :func:`~dkit.plot2.theme.get_theme` as
    :meth:`Theme.apply` stands to :meth:`Theme.context`: reach for it in a
    script, a notebook, or an application's startup, and pass ``theme=``
    explicitly in library code.  The returned previous value makes save and
    restore a single line, which is what a test needs::

        previous = set_default_theme("dkit-dark")
        ...
        set_default_theme(previous)

    args:
        spec: a Theme, the name of one, or None to restore
            :data:`~dkit.plot2.theme.DEFAULT_THEME`.  Resolved immediately, so an unknown name
            raises here rather than at the first render.

    returns:
        the spec that was in effect before this call
    """
    global _default
    resolved = DEFAULT_THEME if spec is None else spec
    get_theme(resolved)
    previous, _default = _default, resolved
    return previous


def default_theme() -> Theme:
    """the theme in effect for a caller who names none

    Equivalent to ``get_theme(None)``, and named for reading at a call site
    where None would be ambiguous.
    """
    return get_theme(_default)


def get_theme(spec: Union[Theme, str, None] = None) -> Theme:
    """coerce a theme spec to a :class:`~dkit.plot2.theme.Theme`

    A string is resolved in this order, first match winning:

    1. a name in the :data:`~dkit.plot2.theme.themes` registry, whether bundled or added by
       :func:`~dkit.plot2.theme.register_theme`
    2. a bundled ``.mplstyle`` file name (see :func:`bundled_styles`), which
       becomes ``Theme(rc=name)``
    3. a matplotlib built in style name (``matplotlib.style.available``), also
       ``Theme(rc=name)``

    The registry comes first, so a project can shadow a bundled name by
    registering over it.  Steps 2 and 3 mean a bare *style* name is accepted
    as shorthand for a theme wrapping it, which is why ``get_theme("ggplot")``
    works without anything being registered.

    args:
        spec: a Theme (returned unchanged), a name resolved as above, or None
            for the default set by :func:`~dkit.plot2.theme.set_default_theme`

    returns:
        Theme instance

    raises:
        DKitPlotException: if a name matches none of the three
    """
    if isinstance(spec, Theme):
        return spec
    if spec is None:
        return current_theme() or default_theme()
    if spec in themes:
        return themes[spec]
    # a bundled or built in *style* name is a reasonable shorthand for a theme
    if _bundled_path(spec) or spec in mpl_style.available:
        return Theme(rc=spec)
    raise DKitPlotException(
        f"unknown theme: {spec}. registered themes: {', '.join(sorted(themes))}. "
        f"bundled styles: {', '.join(bundled_styles())}"
    )
