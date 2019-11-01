from dkit.data import aggregation as agg
import random
import tabulate

sample_data = [
    {
        "month_id": random.randint(0, 12),
        "day_id": random.randint(0, 30),
        "sales": random.triangular(100, 250, 500),
        "cost": random.triangular(90, 200, 250),
    }
    for i in range(10000)
]


a = agg.Aggregate() \
    + agg.GroupBy('month_id', 'day_id') \
    + agg.Sum("sales").alias("tot_sales") \
    + agg.Mean("cost").alias("mean_cost") \
    + agg.Median("cost") \
    + agg.IQR("cost") \
    + agg.Std("cost").alias("cost_std") \
    + agg.Count("cost") \
    + agg.Quantile("cost", .95) \
    + agg.OrderBy("month_id", "day_id").reverse()

print(tabulate.tabulate(list(a(sample_data))[:20], headers="keys"))
