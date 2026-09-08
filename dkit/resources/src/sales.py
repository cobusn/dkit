import sys; sys.path.insert(0, "..")  # noqa
from functools import lru_cache

from dkit.data import manipulate as mp
from dkit.data.fake_helper import sales_transactions
from dkit.doc2 import document as doc
from dkit.doc2.builder import DocumentCode
from dkit.plot2 import quick, scale


PLOTTYPE = ".pdf"


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
        for row in agg:
            mid = row["month_id"]
            row["month_id"] = f"{mid // 10000}-{(mid // 100) % 100:02d}"
        return quick.line(
            agg, x="month_id", y="revenue",
            title="Sales Revenue per Month", ylabel="sales",
            xscale=scale.Categorical("month", rotation=45),
            width=17, height=6,
        )

    @doc.wrap_json
    def table(self):
        top_n = self.variables["top_n"]
        table = doc.Table(
            self.data[:top_n],
            [
                doc.Column("date", "Date", width=2),
                doc.Column("region", "Region", width=5, align="r"),
                doc.Column("product_category", "Category", width=3, align="r"),
                doc.Column("units", "Units", width=2, align="r"),
                doc.Column("revenue", "Revenue", width=2, align="right", format_="R {0:.2f}"),
            ],
            align="center"
        )
        return table
