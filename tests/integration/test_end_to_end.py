from datetime import datetime

from mitzu.datasource_discovery import EventDatasourceDiscovery
from mitzu.model import (
    ConversionMetric,
    DatasetDiscoveryConfig,
    DiscoveredEventDataSource,
    Segment,
)
from tests.helper import assert_row, assert_sql
from tests.samples.sources import get_simple_big_data, get_simple_csv


def test_simple_big_data_discovery():
    discovery = EventDatasourceDiscovery(
        get_simple_big_data(),
        DatasetDiscoveryConfig(datetime(2019, 1, 1), datetime(2022, 1, 1)),
    )
    m = discovery.discover_datasource().create_notebook_class_model()

    seg: Segment = m.app_install.config(
        start_dt="2021-01-01", end_dt="2022-01-01", time_group="total"
    )
    df = seg.get_df()
    print(df)
    assert 1 == df.shape[0]
    assert_row(df, _unique_user_count=2254, _datetime=None, _event_count=4706)


def test_discovered_dataset_pickle():
    config = DatasetDiscoveryConfig(
        start_date=datetime(2019, 1, 1), end_date=datetime(2022, 1, 1)
    )
    discovery = EventDatasourceDiscovery(get_simple_csv(), config)
    dd1 = discovery.discover_datasource()
    dd1.save_project("test_app")

    dd2 = DiscoveredEventDataSource.load_from_project_file("test_app")
    m = dd2.create_notebook_class_model()

    seg: Segment = m.cart.config(
        start_dt="2020-01-01", end_dt="2021-01-01", time_group="total"
    )
    assert 1 == seg.get_df().shape[0]
    assert_row(seg.get_df(), _unique_user_count=108, _datetime=None, _event_count=787)


def test_simple_csv_segmentation():
    config = DatasetDiscoveryConfig(
        start_date=datetime(2019, 1, 1), end_date=datetime(2022, 1, 1)
    )
    discovery = EventDatasourceDiscovery(get_simple_csv(), config)
    m = discovery.discover_datasource().create_notebook_class_model()

    seg: Segment = m.cart.config(
        start_dt="2020-01-01", end_dt="2021-01-01", time_group="total"
    )
    print(seg.get_sql())
    assert_sql(
        """
with anon_2 as (SELECT simple.user_id as _cte_user_id,
                       simple.event_time as _cte_datetime,
                       null as _cte_group
                FROM   simple
                WHERE  simple.event_type = 'cart')
SELECT null as _datetime,
       null as _group,
       count(distinct anon_1._cte_user_id) as _unique_user_count,
       count(anon_1._cte_user_id) as _event_count
FROM   anon_2 as anon_1
WHERE  anon_1._cte_datetime >= '2020-01-01 00:00:00'
   and anon_1._cte_datetime <= '2021-01-01 00:00:00'
GROUP BY _datetime, _group""",
        seg.get_sql(),
    )

    assert 1 == seg.get_df().shape[0]

    assert_row(seg.get_df(), _unique_user_count=108, _datetime=None, _event_count=787)


def test_simple_csv_funnel():
    config = DatasetDiscoveryConfig(
        start_date=datetime(2019, 1, 1), end_date=datetime(2022, 1, 1)
    )
    discovery = EventDatasourceDiscovery(get_simple_csv(), config)
    m = discovery.discover_datasource().create_notebook_class_model()

    conv: ConversionMetric = (m.view >> m.cart).config(
        conv_window="1 month",
        time_group="day",
        start_dt="2020-01-01",
        end_dt="2021-01-01",
        group_by=m.view.category_id,
    )

    assert_sql(
        """
with anon_1 as (SELECT simple.user_id as _cte_user_id,
                       simple.event_time as _cte_datetime,
                       simple.category_id as _cte_group
                FROM   simple
                WHERE  simple.event_type = 'view'), 
anon_2 as (SELECT simple.user_id as _cte_user_id,
                        simple.event_time as _cte_datetime,
                        null as _cte_group
                FROM   simple
                WHERE  simple.event_type = 'cart')
SELECT datetime(strftime('%Y-%m-%dT00:00:00', anon_1._cte_datetime)) as _datetime,
       anon_1._cte_group as _group,
       (count(distinct anon_2._cte_user_id) * 100.0) / count(distinct anon_1._cte_user_id) as _conversion_rate,
       count(distinct anon_1._cte_user_id) as _unique_user_count_1,
       count(anon_1._cte_user_id) as _event_count_1,
       count(distinct anon_2._cte_user_id) as _unique_user_count_2,
       count(anon_2._cte_user_id) as _event_count_2
FROM   anon_1 left
    OUTER JOIN anon_2
        ON anon_1._cte_user_id = anon_2._cte_user_id and
           anon_2._cte_datetime > anon_1._cte_datetime and
           anon_2._cte_datetime <= datetime(anon_1._cte_datetime, '+1 month')
WHERE  anon_1._cte_datetime >= '2020-01-01 00:00:00'
   and anon_1._cte_datetime <= '2021-01-01 00:00:00'
GROUP BY _datetime, _group""",
        conv.get_sql(),
    )

    assert 216 == conv.get_df().shape[0]

    assert_row(
        conv.get_df(),
        _datetime=datetime(2020, 1, 1, 0, 0, 0),
        _group=1487580004857414477,
        _conversion_rate=0,
        _unique_user_count_1=2,
        _event_count_1=2,
        _unique_user_count_2=0,
        _event_count_2=0,
    )
