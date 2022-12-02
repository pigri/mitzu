[![PyPI version](https://badge.fury.io/py/mitzu.svg)](https://badge.fury.io/py/mitzu)
![Mit - License](https://img.shields.io/pypi/l/mitzu)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/mitzu.svg)](https://pypi.org/project/mitzu/)

<h2 align="center">
<b>Mitzu<b> is an open source <b>product analytics </b> tool that queries your <b>data lake</b> or <b>data warehouse</b>.
</h2>
</br>

![webapp example](https://raw.githubusercontent.com/mitzu-io/mitzu/main/resources/mitzu_webapp_hero.gif)

</br>

# Features

- Visualization for:
  - Funnels
  - Segmentation
  - Retention
  - User Journey (coming soon)
  - Revenue calculations (coming soon)
- User Lookup (coming soon)
- Cohorts Analysis
- Standalone web app for non-tech people
- Notebook visual app
- Notebook low-code analytics in python
- Batch ETL jobs support

# Supported Integrations

Mitzu integrates with most modern data lake and warehouse solutions:

- [AWS Athena](https://aws.amazon.com/athena/?whats-new-cards.sort-by=item.additionalFields.postDateTime&whats-new-cards.sort-order=desc)
- [Databricks Spark (SQL)](https://www.databricks.com/product/databricks-sql)
- Files - [SQLite](https://www.sqlite.org/index.html) (csv, parquet, json, etc.)
- [MySQL](https://www.mysql.com/)
- [PostgreSQL](https://www.postgresql.org/)
- [Snowflake](https://www.snowflake.com/en/)
- [Trino / Starburst](https://trino.io/)

## Coming Soon

- [Clickhouse](https://clickhouse.com/)
- [BigQuery](https://cloud.google.com/bigquery/)
- [Redshift](https://aws.amazon.com/redshift/)

# Quick Start

In this section, we will describe how to start with `Mitzu` on your local machine. Skip this section if you rather see `Mitzu` in a prepared notebook or webapp. Otherwise get ready and fire up your own data-science [notebook](https://jupyter.org/).
![Notebook example](https://raw.githubusercontent.com/mitzu-io/mitzu/main/resources/mitzu_notebook_hero.gif)

---

Install the Mitzu python library

```
pip install mitzu
```

## Reading The Sample Dataset

The simplest way to get started with `Mitzu` is in a data-science notebook. In your notebook read the sample user behavior dataset.
Mitzu can [discover](https://mitzu.io/documentation/discovery) your tables in a data warehouse or data lake. For the sake of simplicity we provide you an in-memory [sqlite](https://www.sqlite.org/index.html) based table that contains

```python
import mitzu.samples as smp

dp = smp.get_sample_discovered_project()
m = dp.create_notebook_class_model()
```

## Segmentation

The following command visualizes the `count of unique users` over time who did `page visit` action in the last `30 days`.

```python
m.page_visit
```

![segmentation metric](https://raw.githubusercontent.com/mitzu-io/mitzu/main/resources/segmentation.png)

In the example above `m.page_visit` refers to a `user event segment` for which the notebook representation is a `segmentation chart`.
If this sounds unfamiliar, don't worry! Later we will explain you everything.

## Funnels

You can create a `funnel chart` by placing the `>>` operator between two `user event segments`.

```python
m.page_visit >> m.checkout
```

This will visualize the `conversion rate` of users that first did `page_visit` action and then did `checkout` within a day in the last 30 days.

## Filtering

You can apply filters to `user event segment` the following way:

```python
m.page_visit.user_country_code.is_us >> m.checkout

# You can achieve the same filter with:
# (m.page_visit.user_country_code == 'us')
#
# you can also apply >, >=, <, <=, !=, operators.
```

With this syntax we have narrowed down our `page visit` `user event segment` to page visits from the `US`.
Stacking filters is possible with the `&` (and) and `|` (or) operators.

```python
m.page_visit.user_country_code.is_us & m.page_visit.acquisition_campaign.is_organic

# if using the comparison operators, make sure you put the user event segments in parenthesis.
# (m.page_visit.user_country_code == 'us') & (m.page_visit.acquisition_campaign == 'organic')
```

Apply multi value filtering with the `any_of` or `none_of` functions:

```python
m.page_visit.user_country_code.any_of('us', 'cn', 'de')

# m.page_visit.user_country_code.none_of('us', 'cn', 'de')
```

Of course you can apply filters on every `user event segment` in a funnel.

```python
m.add_to_cart >> (m.checkout.cost_usd <= 1000)
```

## Metrics Configuration

To any funnel or segmentation you can apply the config method. Where you can define the parameters of the metric.

```python
m.page_visit.config(
   start_dt="2021-08-01",
   end_dt="2021-09-01",
   group_by=m.page_visit.domain,
   time_group='total',
)
```

- `start_dt` should be an iso datetime string, or python datetime, where the metric should start.
- `end_dt` should be an iso datetime string, or python datetime, where the metric should end.
- `group_by` is a property that you can refer to from the notebook class model.
- `time_group` is the time granularity of the query for which the possible values are: `hour`, `day`, `week`, `month`, `year`, `total`

Funnels have an extra configuration parameter `conv_window`, this has the following format: `<VAL> <TIME WINDOW>`, where `VAL` is a positive integer.

```python
(m.page_visit >> m.checkout).config(
   start_dt="2021-08-01",
   end_dt="2021-09-01",
   group_by=m.page_visit.domain,
   time_group='total',
   conv_window='1 day',
)
```

## SQL Generator

For any metric you can print out the SQL code that `Mitzu` generates.
This you can do by calling the `.print_sql()` method.

```python
(m.page_visit >> m.checkout).config(
   start_dt="2021-08-01",
   end_dt="2021-09-01",
   group_by=m.page_visit.domain,
   time_group='total',
   conv_window='1 day',
).print_sql()
```

![webapp example](https://raw.githubusercontent.com/mitzu-io/mitzu/main/resources/print_sql.png)

## Pandas DataFrames

Similarly you can access the results in the form of a [Pandas](https://pandas.pydata.org/) DataFrame with the method `.get_df()`

```python
(m.page_visit >> m.checkout).config(
   start_dt="2021-08-01",
   end_dt="2021-09-01",
   group_by=m.page_visit.domain,
   time_group='total',
   conv_window='1 day',
).get_df()
```

## Notebook Dashboards

You can also visualize the webapp in a Jupyter Notebook:

```python
import mitzu.samples as smp

dp = smp.get_sample_discovered_project()
dp.notebook_dashboard()
```

![dash](https://raw.githubusercontent.com/mitzu-io/mitzu/main/resources/dash_notebook.png)

## Usage In Notebooks

- [Example notebook](https://deepnote.com/@istvan-meszaros/Mitzu-Introduction-af037f5a-2184-494d-9362-6f4c69b5eedc)
- [Documentation](https://mitzu.io/documentation/notebook)

## Webapp

Mitzu can run as a standalone webapp or embedded inside a notebook.

Trying out locally:

```bash
docker run -p 8082:8082 imeszaros/mitzu-webapp
```

- [Example webapp](https://app.mitzu.io)
- [Webapp documentation](https://mitzu.io/documentation/webapp)

## Connect Your Own Data

Mitzu is be able to connect to your data warehouse or data lake.
To get started with your own data integration please read our handy
[docs](/DOCS.md)

## Contribution Guide

Please read our [Contribution Guide](/CONTRIBUTION.md)
