from datetime import datetime

from mitzu.model import ConversionMetric, RetentionMetric, Segment
from mitzu.project_discovery import ProjectDiscovery
from tests.helper import assert_row, assert_sql
from tests.samples.sources import get_simple_csv


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
  (SELECT main.simple.user_id AS _cte_user_id,
          main.simple.event_time AS _cte_datetime,
          NULL AS _cte_group
   FROM main.simple
   WHERE main.simple.event_type = 'cart'
     AND main.simple.event_time >= '2020-01-01 00:00:00'
     AND main.simple.event_time <= '2021-01-01 00:00:00'
     AND date(main.simple.event_time) >= date('2020-01-01')
     AND date(main.simple.event_time) <= date('2021-01-01'))
SELECT NULL AS _datetime,
       NULL AS _group,
       count(DISTINCT anon_1._cte_user_id) AS _agg_value
FROM anon_2 AS anon_1
GROUP BY _datetime,
         _group""",
        seg.get_sql(),
    )

    assert 1 == seg.get_df().shape[0]

    assert_row(seg.get_df(), _agg_value=108, _datetime=None)


def test_simple_csv_funnel():

    discovery = ProjectDiscovery(get_simple_csv())
    m = discovery.discover_project().create_notebook_class_model()

    conv: ConversionMetric = (m.view.group_by(m.view.category_id) >> m.cart).config(
        conv_window="1 month",
        time_group="day",
        start_dt="2020-01-01",
        end_dt="2021-01-01",
    )

    conv.print_sql()
    assert_sql(
        """
WITH anon_1 AS
  (SELECT main.simple.user_id AS _cte_user_id,
          main.simple.event_time AS _cte_datetime,
          main.simple.category_id AS _cte_group
   FROM main.simple
   WHERE main.simple.event_type = 'view'
     AND main.simple.event_time >= '2020-01-01 00:00:00'
     AND main.simple.event_time <= '2021-01-01 00:00:00'
     AND date(main.simple.event_time) >= date('2020-01-01')
     AND date(main.simple.event_time) <= date('2021-01-01')),
     anon_2 AS
  (SELECT main.simple.user_id AS _cte_user_id,
          main.simple.event_time AS _cte_datetime,
          NULL AS _cte_group
   FROM main.simple
   WHERE main.simple.event_type = 'cart'
     AND main.simple.event_time >= '2020-01-01 00:00:00'
     AND main.simple.event_time <= '2021-02-01 00:00:00'
     AND date(main.simple.event_time) >= date('2020-01-01')
     AND date(main.simple.event_time) <= date('2021-02-01'))
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
GROUP BY _datetime,
         _group""",
        conv.get_sql(),
    )

    assert 216 == conv.get_df().shape[0]


def test_null_filter():
    discovery = ProjectDiscovery(get_simple_csv())
    m = discovery.discover_project().create_notebook_class_model()

    sql = m.view.category_id.is_not_null.get_sql()
    print(sql)
    assert (
        """WITH anon_2 AS
  (SELECT main.simple.user_id AS _cte_user_id,
          main.simple.event_time AS _cte_datetime,
          NULL AS _cte_group
   FROM main.simple
   WHERE main.simple.event_type = 'view'
     AND main.simple.event_time >= '2021-12-02 00:00:00'
     AND main.simple.event_time <= '2022-01-01 00:00:00'
     AND date(main.simple.event_time) >= date('2021-12-02')
     AND date(main.simple.event_time) <= date('2022-01-01')
     AND main.simple.category_id IS NOT NULL)
SELECT datetime(strftime('%Y-%m-%dT00:00:00', anon_1._cte_datetime)) AS _datetime,
       NULL AS _group,
       count(DISTINCT anon_1._cte_user_id) AS _agg_value
FROM anon_2 AS anon_1
GROUP BY _datetime,
         _group"""
        == sql
    )


def test_retention_query():
    discovery = ProjectDiscovery(get_simple_csv())
    m = discovery.discover_project().create_notebook_class_model()

    conv: RetentionMetric = (m.view >= m.cart).config(
        retention_window="1 hour",
        time_group="hour",
        start_dt="2020-01-01",
        end_dt="2020-01-02",
        resolution="one_user_event_per_minute",
    )

    print(conv.get_df())

    assert_row(
        conv.get_df(),
        _datetime=datetime(2020, 1, 1, 0, 0, 0),
        _ret_index=0,
        _user_count_1=160,
        _user_count_2=19,
        _agg_value=11.875,
    )

    sql = conv.get_sql()
    print(sql)
    assert (
        """WITH anon_1 AS
  (SELECT DISTINCT main.simple.user_id AS _cte_user_id,
                   datetime(strftime('%Y-%m-%dT%H:%M:00', main.simple.event_time)) AS _cte_datetime,
                   NULL AS _cte_group
   FROM main.simple
   WHERE main.simple.event_type = 'view'
     AND main.simple.event_time >= '2020-01-01 00:00:00'
     AND main.simple.event_time <= '2020-01-02 00:00:00'
     AND date(main.simple.event_time) >= date('2020-01-01')
     AND date(main.simple.event_time) <= date('2020-01-02')),
     anon_3 AS
  (SELECT 0 AS _ret_index
   UNION SELECT 1 AS _ret_index
   UNION SELECT 2 AS _ret_index
   UNION SELECT 3 AS _ret_index
   UNION SELECT 4 AS _ret_index
   UNION SELECT 5 AS _ret_index
   UNION SELECT 6 AS _ret_index
   UNION SELECT 7 AS _ret_index
   UNION SELECT 8 AS _ret_index
   UNION SELECT 9 AS _ret_index
   UNION SELECT 10 AS _ret_index
   UNION SELECT 11 AS _ret_index
   UNION SELECT 12 AS _ret_index
   UNION SELECT 13 AS _ret_index
   UNION SELECT 14 AS _ret_index
   UNION SELECT 15 AS _ret_index
   UNION SELECT 16 AS _ret_index
   UNION SELECT 17 AS _ret_index
   UNION SELECT 18 AS _ret_index
   UNION SELECT 19 AS _ret_index
   UNION SELECT 20 AS _ret_index
   UNION SELECT 21 AS _ret_index
   UNION SELECT 22 AS _ret_index
   UNION SELECT 23 AS _ret_index
   UNION SELECT 24 AS _ret_index),
     anon_2 AS
  (SELECT DISTINCT main.simple.user_id AS _cte_user_id,
                   datetime(strftime('%Y-%m-%dT%H:%M:00', main.simple.event_time)) AS _cte_datetime,
                   NULL AS _cte_group
   FROM main.simple
   WHERE main.simple.event_type = 'cart'
     AND main.simple.event_time >= '2020-01-01 00:00:00'
     AND main.simple.event_time <= '2020-01-02 01:00:00'
     AND date(main.simple.event_time) >= date('2020-01-01')
     AND date(main.simple.event_time) <= date('2020-01-02'))
SELECT datetime(strftime('%Y-%m-%dT%H:00:00', anon_1._cte_datetime)) AS _datetime,
       NULL AS _group,
       ret_indeces._ret_index,
       count(DISTINCT anon_1._cte_user_id) AS _user_count_1,
       count(DISTINCT anon_2._cte_user_id) AS _user_count_2,
       (count(DISTINCT anon_2._cte_user_id) * 100.0) / count(DISTINCT anon_1._cte_user_id) AS _agg_value
FROM anon_1
JOIN anon_3 AS ret_indeces ON TRUE
LEFT OUTER JOIN anon_2 ON anon_1._cte_user_id = anon_2._cte_user_id
AND datetime(anon_2._cte_datetime) > datetime(anon_1._cte_datetime, '+' || ret_indeces._ret_index || ' hour')
AND datetime(anon_2._cte_datetime) <= datetime(datetime(anon_1._cte_datetime, '+' || ret_indeces._ret_index || ' hour'), '+1 hour')
GROUP BY _datetime,
         _group,
         _ret_index"""
        == sql
    )
