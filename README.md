[![PyPI version](https://badge.fury.io/py/mitzu.svg)](https://badge.fury.io/py/mitzu)
![Mit - License](https://img.shields.io/pypi/l/mitzu)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/mitzu.svg)](https://pypi.org/project/mitzu/)
[![codecov](https://codecov.io/gh/mitzu-io/mitzu/branch/main/graph/badge.svg?token=A1HQLBLSUA)](https://codecov.io/gh/mitzu-io/mitzu)

<h2 align="center">
<b>Mitzu<b> is an open source <b>product analytics </b> tool over your company's <b>data warehouse</b>.
</h2>
</br>

![webapp example](https://raw.githubusercontent.com/mitzu-io/mitzu/main/resources/mitzu_webapp_hero.gif)

</br>

# Features

- Friendly Web UI that enables product analytics over your data warehouse or data lake. (Similar to Amplitude, Mixpanel, Heap and Google Analytics)
- Easy setup in under a minute.
- Discovers your company's data warehouse for user events and event properties.

- Metrics such as:
  - User Funnels
  - Event Segmentation
  - Retention
  - User Journey (coming soon)
  - Revenue calculations (MRR, ARR) (coming soon)
  - Churn rate (coming soon)
- User Lookup (coming soon)
- Cohorts Analysis (coming soon)
- A/B Test Evaluation (coming soon)
- Saved metrics
- Dashboards

- User Authentication
  - Internal user management
  - OAuth2 - (OIDC)

# Supported Warehouse Integrations

Mitzu integrates with most modern data lake and warehouse solutions:

- [AWS Athena](https://aws.amazon.com/athena/?whats-new-cards.sort-by=item.additionalFields.postDateTime&whats-new-cards.sort-order=desc)
- [Databricks Spark (SQL)](https://www.databricks.com/product/databricks-sql)
- [MySQL](https://www.mysql.com/)
- [PostgreSQL](https://www.postgresql.org/)
- [Snowflake](https://www.snowflake.com/en/)
- [Trino / Starburst](https://trino.io/)
- [Redshift](https://aws.amazon.com/redshift/)

## Coming Soon

- [Clickhouse](https://clickhouse.com/)
- [BigQuery](https://cloud.google.com/bigquery/)

# Getting started

Mitzu can run as a standalone webapp:

Trying out locally:

```zsh
docker run -p 8080:8080 mitzuio/mitzu:latest
```

- [Beta webapp](https://beta.mitzu.io)
- [Documentation](https://mitzu.io/documentation/)

## Contribution Guide

Please read our [Contribution Guide](/CONTRIBUTION.md)
