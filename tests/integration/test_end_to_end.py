from datetime import datetime

from mitzu.model import ConversionMetric, DiscoveredProject, Segment
from mitzu.project_discovery import ProjectDiscovery
from tests.helper import assert_row, assert_sql
from tests.samples.sources import get_simple_big_data, get_simple_csv


def test_simple_big_data_discovery():
    project = get_simple_big_data()
    discovery = ProjectDiscovery(project)
    m = discovery.discover_project().create_notebook_class_model()

    seg: Segment = m.app_install.config(
        start_dt="2021-01-01",
        end_dt="2022-01-01",
        time_group="total",
    )
    df = seg.get_df()
    print(df)
    assert 1 == df.shape[0]
    assert_row(df, _agg_value=2254, _datetime=None)

    seg: Segment = m.app_install.config(
        start_dt="2021-01-01",
        end_dt="2022-01-01",
        time_group="total",
        aggregation="event_count",
    )
    df = seg.get_df()
    print(df)
    assert 1 == df.shape[0]
    assert_row(df, _agg_value=4706, _datetime=None)


def test_discovered_dataset_pickle():

    discovery = ProjectDiscovery(project=get_simple_csv())
    dd1 = discovery.discover_project()
    dd1.save_to_project_file("test_app")

    dd2 = DiscoveredProject.load_from_project_file("test_app")
    m = dd2.create_notebook_class_model()

    seg: Segment = m.cart.config(
        start_dt="2020-01-01", end_dt="2021-01-01", time_group="total"
    )
    assert 1 == seg.get_df().shape[0]
    assert_row(seg.get_df(), _agg_value=108, _datetime=None)


def test_simple_csv_segmentation():
    discovery = ProjectDiscovery(get_simple_csv())
    m = discovery.discover_project().create_notebook_class_model()

    seg: Segment = m.cart.config(
        start_dt="2020-01-01", end_dt="2021-01-01", time_group="total"
    )
    print(seg.get_sql())
    assert_sql(
        """
WITH anon_2 AS
  (SELECT simple.user_id AS _cte_user_id,
          simple.event_time AS _cte_datetime,
          NULL AS _cte_group
   FROM SIMPLE
   WHERE simple.event_type = 'cart'
     AND date(simple.event_time) >= date('2020-01-01')
     AND date(simple.event_time) <= date('2021-01-01'))
SELECT NULL AS _datetime,
       NULL AS _group,
       count(DISTINCT anon_1._cte_user_id) AS _agg_value
FROM anon_2 AS anon_1
WHERE anon_1._cte_datetime >= '2020-01-01 00:00:00'
  AND anon_1._cte_datetime <= '2021-01-01 00:00:00'
GROUP BY _datetime,
         _group""",
        seg.get_sql(),
    )

    assert 1 == seg.get_df().shape[0]

    assert_row(seg.get_df(), _agg_value=108, _datetime=None)


def test_simple_csv_funnel():

    discovery = ProjectDiscovery(get_simple_csv())
    m = discovery.discover_project().create_notebook_class_model()

    conv: ConversionMetric = (m.view >> m.cart).config(
        conv_window="1 month",
        time_group="day",
        start_dt="2020-01-01",
        end_dt="2021-01-01",
        group_by=m.view.category_id,
    )

    conv.print_sql()
    assert_sql(
        """
WITH anon_1 AS
  (SELECT simple.user_id AS _cte_user_id,
          simple.event_time AS _cte_datetime,
          simple.category_id AS _cte_group
   FROM SIMPLE
   WHERE simple.event_type = 'view'
     AND date(simple.event_time) >= date('2020-01-01')
     AND date(simple.event_time) <= date('2021-02-01')),
     anon_2 AS
  (SELECT simple.user_id AS _cte_user_id,
          simple.event_time AS _cte_datetime,
          NULL AS _cte_group
   FROM SIMPLE
   WHERE simple.event_type = 'cart'
     AND date(simple.event_time) >= date('2020-01-01')
     AND date(simple.event_time) <= date('2021-02-01'))
SELECT datetime(strftime('%Y-%m-%dT00:00:00', anon_1._cte_datetime)) AS _datetime,
       anon_1._cte_group AS _group,
       count(DISTINCT anon_1._cte_user_id) AS _user_count_1,
       (count(DISTINCT anon_1._cte_user_id) * 100.0) / count(DISTINCT anon_1._cte_user_id) AS _agg_value_1,
       count(DISTINCT anon_2._cte_user_id) AS _user_count_2,
       (count(DISTINCT anon_2._cte_user_id) * 100.0) / count(DISTINCT anon_1._cte_user_id) AS _agg_value_2
FROM anon_1
LEFT OUTER JOIN anon_2 ON anon_1._cte_user_id = anon_2._cte_user_id
AND anon_2._cte_datetime > anon_1._cte_datetime
AND anon_2._cte_datetime <= datetime(anon_1._cte_datetime, '+1 month')
WHERE anon_1._cte_datetime >= '2020-01-01 00:00:00'
  AND anon_1._cte_datetime <= '2021-01-01 00:00:00'
GROUP BY _datetime,
         _group""",
        conv.get_sql(),
    )

    assert 216 == conv.get_df().shape[0]

    assert_row(
        conv.get_df(),
        _datetime=datetime(2020, 1, 1, 0, 0, 0),
        _group=1487580004857414477,
        _user_count_1=2,
        _agg_value_1=100,
        _user_count_2=0,
        _agg_value_2=0,
    )

    sql = m.view.category_id.is_not_null.get_sql()
    print(sql)
    assert (
        """WITH anon_2 AS
  (SELECT simple.user_id AS _cte_user_id,
          simple.event_time AS _cte_datetime,
          NULL AS _cte_group
   FROM SIMPLE
   WHERE simple.event_type = 'view'
     AND date(simple.event_time) >= date('2021-12-02')
     AND date(simple.event_time) <= date('2022-01-01')
     AND simple.category_id IS NOT NULL)
SELECT datetime(strftime('%Y-%m-%dT00:00:00', anon_1._cte_datetime)) AS _datetime,
       NULL AS _group,
       count(DISTINCT anon_1._cte_user_id) AS _agg_value
FROM anon_2 AS anon_1
WHERE anon_1._cte_datetime >= '2021-12-02 00:00:00'
  AND anon_1._cte_datetime <= '2022-01-01 00:00:00'
GROUP BY _datetime,
         _group"""
        == sql
    )
