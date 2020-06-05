from abc import ABC, abstractmethod
from typing import Union
import os

from . import VALID_TERMINALS
from ..utilities.mixins import SerDeMixin
from ..exceptions import CkitPlotException, CkitArgumentException


Num = Union[int, float]


class PlotBase(ABC, SerDeMixin):
    """base class for plots"""

    def __init__(self, data, *args, **kwargs):
        self.data = list(data)
        self.series = []
        self.title = None
        self.axes = {"0": {}, "1": {}}
        self.aes = Aesthetic()

    def has_boxes(self) -> bool:
        """Return true if plot has any box plots such as histograms"""
        classnames = [i.primitive_type() for i in self.series]
        if "box" in classnames:
            return True
        else:
            return False

    @staticmethod
    def terminal_from_filename(filename):
        """
        determine terminal from filename

        args:
            filename

        returns:
            valid terminal name

        raises:
            PlotError

        """
        filename, extension = os.path.splitext(filename)
        extension = extension.lower().strip(".")
        if extension in VALID_TERMINALS:
            return extension
        else:
            raise CkitPlotException(f"Invalid plot Extension {extension}.")

    def __add__(self, other):
        """modify metadata"""
        other.modify(self)
        return self


class PlotModifier(SerDeMixin):
    """base class for plot modifiers"""

    def modify(self, plot):
        plot.series.append(self)


class Plot(PlotBase):
    """
    Base class for plot data structure
    """
    pass


class Adornment(PlotModifier):
    """Custom adornments, e.g. horizontal line"""
    def modify(self, plot):
        plot.series.append(self)


class __TextAdornment(Adornment):
    """Add text to plot"""
    def __init__(self, text, location='best', alpha=None, size=None):
        self.text = text
        self.location = location
        self.alpha = alpha
        self.size = size


class AnchoredText(__TextAdornment):
    pass


class LineAdornment(Adornment):

    def __init__(self, color: str = None, line_style: str = None, line_width: float = None,
                 alpha: float = None, **kwargs):
        super().__init__()
        self.line_style = line_style
        self.color = color
        self.line_width = line_width
        self.alpha = alpha


class HLine(LineAdornment):

    def __init__(self, y, color: str = None, line_style: str = None, line_width: float = None,
                 alpha: float = None, **kwargs):
        super().__init__(color=color, line_style=line_style, line_width=line_width,
                         alpha=alpha, **kwargs)
        self.y = y


class VLine(LineAdornment):

    def __init__(self, x, color: str = None, line_style: str = None, line_width: float = None,
                 alpha: float = None, **kwargs):
        super().__init__(color=color, line_style=line_style, line_width=line_width,
                         alpha=alpha, **kwargs)
        self.x = x


class AbstractGeom(PlotModifier):
    """
    arguments:
        * title: plot title
        * y_data: y data field name
        * x_data: x data field name
        * color: color for this series
        * alpha: alpha for color
        * y_range limit y to range
    """
    def __init__(self, title: str,  x: str, y: str, color: str = None,
                 alpha: float = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.title = title
        self.x_data = x
        self.y_data = y
        self.color = color
        self.alpha = alpha

    @abstractmethod
    def primitive_type(self):
        """primitive type of graph (e.g line)"""
        pass


class GeomBar(AbstractGeom):

    def __init__(self, title: str,  x_data: str, y_data: str, color: str = None,
                 alpha: float = None, *args, **kwargs):
        super().__init__(title, x_data, y_data, color, alpha, *args, **kwargs)
        self.primitive_type = "bar"
        self.isbox = True


class GeomDelta(GeomBar):
    """Barplot with red styling for positive and green for negative"""
    pass


class GeomTreeMap(GeomBar):

    def __init__(self, title: str,  x_data: str, y_data: str, color: str = None,
                 alpha: float = None, str_format=None, *args, **kwargs):
        super().__init__(title, x_data, y_data, color, alpha, *args, **kwargs)
        self.str_format = str_format


class GeomImpulse(GeomBar):
    pass


class GeomHistogram(GeomBar):
    """
    Plot a frequency distribution
    """
    def __init__(self, title: str, color: str = None, alpha: float = None):
        super().__init__(title, "left", "count", color, alpha)


class GeomLine(AbstractGeom):

    def __init__(self, title: str,  x_data: str, y_data: str, color: str = None,
                 alpha: float = None, *args, **kwargs):
        super().__init__(title, x_data, y_data, color, alpha, *args, **kwargs)
        self.primitive_type = "line"


class GeomFill(AbstractGeom):
    """
    Fill between two lines
    """
    def __init__(self, title: str,  x_data: str, y_upper: str, y_lower: str,
                 color: str = "green", line_alpha: float = 0.8,
                 fill_alpha: float = 0.3, *args, **kwargs):
        self.primitive_type = "line"
        self.title = title
        self.x_data = x_data
        self.y_upper = y_upper
        self.y_lower = y_lower
        self.fill_alpha = fill_alpha
        self.line_alpha = line_alpha
        self.color = color


class GeomArea(GeomLine):
    pass


class GeomCumulative(AbstractGeom):
    """
    Add cumulative line on right hand axis
    """
    def __init__(self, title: str, y_data: str, color: str = None,
                 alpha: float = None, *args, **kwargs):
        super().__init__(title, x=None, y=y_data, color=color,
                         alpha=alpha, *args, **kwargs)
        self.primitive_type = "line"

#  class PlotBoxplot(PlotLine):
#    pass
#  https://stackoverflow.com/questions/15404628/how-can-i-generate-box-and-whisker-plots-with-variable-box-width-in-gnuplot
#


class GeomScatter(AbstractGeom):

    def __init__(self, title: str,  x_data: str, y_data: str, color: str = None,
                 alpha: float = None, *args, **kwargs):
        super().__init__(title, x_data, y_data, color, alpha, *args, **kwargs)
        self.primitive_type = "point"


class Abstract3DGeom(AbstractGeom):
    """
    arguments:
        * title: plot title
        * y_data: field name for y data
        * x_data: field name for x data
        * color: color for this series
        * alpha: alpha for color
        * y_range limit y to range
    """
    def __init__(self, title: str,  x: str, y: str, z: str,
                 color: str = None, alpha: float = None, *args, **kwargs):
        super().__init__(title, x, y, color, alpha, *args, **kwargs)
        self.z_data = z
        self.primitive_type = "3d"


class GeomHeatMap(Abstract3DGeom):
    pass


class Title(PlotModifier):

    def __init__(self, title, sub_title=None):
        self.title = title
        self.sub_title = sub_title

    def modify(self, plot):
        plot.title = self.title
        plot.sub_title = self.sub_title


class _Axis(PlotModifier):
    """axis specification

    arguments:
        title: axis title
        min: minimum value
        max: maximum value
        rotate: rotate labels
        float_format: string.format formatting
        defeat: supress addititiona axis formatting
    """
    def __init__(self, title, min=None, max=None, rotation=None, float_format=None,
                 time_format=None, which="0", defeat=False, *args, **kwargs):
        self.title = title
        self.min_val = min
        self.max_val = max
        self.rotation = rotation
        if all([time_format, float_format]):
            raise CkitArgumentException("float_format and time_format is mutually exclusive")
        self.float_format = float_format
        self.time_format = time_format
        self.defeat = defeat
        if str(which) in ["0", "1", "2"]:
            self.which = str(which)
        else:
            raise CkitPlotException("Axis number can only be 0, 1 or 2")

    def modify(self, plot):
        plot.axes[self.which] = self


class XAxis(_Axis):
    """annotate the Y axis"""
    pass


class YAxis(_Axis):
    """Annotate the X axis"""

    def __init__(self, title, min=None, max=None, rotation=None, float_format=None,
                 *args, **kwargs):
        super().__init__(title, min, max, rotation=rotation, float_format=float_format,
                         which=1, *args, **kwargs)


class ZAxis(_Axis):
    """Annotate the X axis"""

    def __init__(self, title, min=None, max=None, rotation=None, float_format=None,
                 *args, **kwargs):
        super().__init__(title, min, max, rotation=rotation, float_format=float_format,
                         which=2, *args, **kwargs)


class Aesthetic(SerDeMixin):
    """
    global settings

    Args:
        stacked: True if bars should be stacked
        boxwidht: relative with of each box
        width: width of resulting plot
        height: height of resulting plot
    """
    def __init__(self, stacked=True, box_width=0.95, width=None, height=None, unit="cm",
                 font="Arial", font_size="12", dpi=96, *args, **kwargs):
        # super().__init__(*args, **kwargs)
        self.stacked = stacked
        self.box_width = box_width
        self.width = width
        self.height = height
        assert unit in ["cm", "inch"]
        self.unit = unit
        self.font = font
        self.font_size = font_size
        self.dpi = dpi
        self.__dpi_2_cmpi = 0.393701
        self.__cm_2_inch = 0.393701

    @property
    def dots_per_cm(self):
        """
        convert DPI to dots per CM
        """
        return int(round(self.dpi * self.__dpi_2_cmpi))

    def __convert_to_pixels(self, size):
        """
        confert size to pixels using stored dpi
        """
        if self.unit == "inch":
            return self.dpi * size
        elif self.unit == "cm":
            return self.dots_per_cm * size

    def _to_inch(self, size):
        """
        confert size to pixels using stored dpi
        """
        if self.unit == "inch":
            return size
        elif self.unit == "cm":
            return self.__cm_2_inch * size

    @property
    def inch_width(self):
        """
        width in inches
        """
        if self.width:
            return self._to_inch(self.width)
        else:
            return None

    @property
    def inch_height(self):
        """
        width in inches
        """
        if self.height:
            return self._to_inch(self.height)
        else:
            return None

    @property
    def pixel_width(self):
        """
        width in pixesl
        """
        return self.__convert_to_pixels(self.width)

    @property
    def pixel_height(self):
        """
        height in pixels
        """
        return self.__convert_to_pixels(self.height)

    def modify(self, plot):
        plot.aes = self


GEOM_MAP = {
    "scatter": GeomScatter,
    "bar": GeomBar,
    "line": GeomLine,
    "area": GeomArea,
    "impulse": GeomImpulse,
}
