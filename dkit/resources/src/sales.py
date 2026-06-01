import sys; sys.path.insert(0, "..")  # noqa
from functools import lru_cache
import matplotlib.pyplot as plt
from dkit.data.fake_helper import sales_transactions
from dkit.doc2 import document as doc
from dkit.doc2.builder import DocumentCode
from dkit.data import manipulate as mp

PLOTTYPE = ".pdf"


def cm(value: float) -> float:
    return value / 2.54


class Sales(DocumentCode):

    @property
    @lru_cache
    def data(self):
        return list(sales_transactions(1000))

    @doc.wrap_matplotlib(filename="plot.pdf")
    def plot(self):
        agg = sorted(
            list(mp.aggregate(self.data, ["month_id"], "revenue")),
            key=lambda x: x["month_id"]
        )
        top_n = self.variables["top_n"]
        agg = agg[-top_n:]
        plt.figure(
            figsize=(cm(17), cm(6))
        )
        plt.plot(
            [str(i["month_id"]) for i in agg],
            [i["revenue"] for i in agg],
        )
        plt.xticks(rotation=45, ha="right")
        plt.xlabel("month")
        plt.ylabel("sales")
        plt.title("Sales Revenue per Month")
        plt.tight_layout()
        return plt

    @doc.wrap_json
    def table(self):
        top_n = self.variables["top_n"]
        table = doc.Table(
            self.data[:top_n],
            [
                doc.Column("date", "Date", width=2),
                doc.Column("region", "Region", width=3, align="r"),
                doc.Column("product_category", "Category", width=3, align="r"),
                doc.Column("units", "Units", width=2, align="r"),
                doc.Column("revenue", "Revenue", width=2, align="right", format_="R {0:.2f}"),
            ],
            align="center"
        )
        return table
