from services.adapters.sqlite_adapter import SQLiteAdapter
from services.discovery.dataset_discovery import EventDatasetDiscovery
from tests.samples.sources import SIMPLE_BIG_DATA, SIMPLE_CSV
from datetime import datetime
from services.notebook.model_loader import ModelLoader
from services.common.model import ConversionMetric, Segment
from tests.helper import assert_row, assert_sql


def test_simple_big_data_discovery():
    adapter = SQLiteAdapter(SIMPLE_BIG_DATA)

    discovery = EventDatasetDiscovery(
        SIMPLE_BIG_DATA, adapter, datetime(2021, 1, 1), datetime(2022, 1, 1)
    )

    dd = discovery.discover_dataset()

    ml = ModelLoader()
    m = ml.create_dataset_model(dd)

    seg: Segment = m.app_install.config(
        start_dt="2021-01-01", end_dt="2022-01-01", time_group="total"
    )

    assert 1 == seg.get_df().shape[0]
    assert_row(seg.get_df(), unique_user_count=2254, datetime=None, event_count=4706)


def test_simple_csv_segmentation():
    adapter = SQLiteAdapter(SIMPLE_CSV)
    discovery = EventDatasetDiscovery(
        SIMPLE_CSV, adapter, datetime(2021, 1, 1), datetime(2022, 1, 1)
    )
    dd = discovery.discover_dataset()
    ml = ModelLoader()
    m = ml.create_dataset_model(dd)

    seg: Segment = m.cart.config(
        start_dt="2020-01-01", end_dt="2021-01-01", time_group="total"
    )
    assert_sql(
        """
        SELECT 
            null as datetime,
            null as "group",
            count(distinct simple_dataset_1.user_id) as unique_user_count,
            count(simple_dataset_1.user_id) as event_count
        FROM   simple_dataset as simple_dataset_1
        WHERE  simple_dataset_1.event_type = 'cart'
            and simple_dataset_1.event_time >= '2020-01-01 00:00:00'
            and simple_dataset_1.event_time <= '2021-01-01 00:00:00'
        GROUP BY null, null  
    """,
        seg.get_sql(),
    )

    assert 1 == seg.get_df().shape[0]

    assert_row(seg.get_df(), unique_user_count=108, datetime=None, event_count=787)


def test_simple_csv_funnel():
    adapter = SQLiteAdapter(SIMPLE_CSV)
    discovery = EventDatasetDiscovery(
        SIMPLE_CSV, adapter, datetime(2021, 1, 1), datetime(2022, 1, 1)
    )
    dd = discovery.discover_dataset()
    ml = ModelLoader()
    m = ml.create_dataset_model(dd)

    conv: ConversionMetric = (m.view >> m.cart).config(
        conv_window="1 month",
        time_group="day",
        start_dt="2020-01-01",
        end_dt="2021-01-01",
        group_by=m.view.category_id,
    )
    print(conv.get_sql())
    assert_sql(
        """
        SELECT datetime(strftime('%Y-%m-%dT00:00:00', simple_dataset_1.event_time)) as datetime,
            simple_dataset_1.category_id as "group",
            (count(distinct simple_dataset_2.user_id) * 1.0) / 
                count(distinct simple_dataset_1.user_id) as conversion_rate,
            count(distinct simple_dataset_1.user_id) as unique_user_count_1,
            count(simple_dataset_1.user_id) as event_count_1,
            count(distinct simple_dataset_2.user_id) as unique_user_count_2,
            count(simple_dataset_2.user_id) as event_count_2
        FROM   simple_dataset as simple_dataset_1 left
            OUTER JOIN simple_dataset as simple_dataset_2
                ON simple_dataset_1.user_id = simple_dataset_2.user_id and
                simple_dataset_2.event_time > simple_dataset_1.event_time and
                simple_dataset_2.event_time <= datetime(simple_dataset_1.event_time, '+1 month') and
                simple_dataset_2.event_type = 'cart'
        WHERE  simple_dataset_1.event_type = 'view'
        and simple_dataset_1.event_time >= '2020-01-01 00:00:00'
        and simple_dataset_1.event_time <= '2021-01-01 00:00:00'
        GROUP BY datetime(strftime('%Y-%m-%dT00:00:00', simple_dataset_1.event_time)), simple_dataset_1.category_id""",
        conv.get_sql(),
    )

    assert 216 == conv.get_df().shape[0]

    assert_row(
        conv.get_df(),
        datetime="2020-01-01 00:00:00",
        group=1487580004857414477,
        conversion_rate=0,
        unique_user_count_1=2,
        event_count_1=2,
        unique_user_count_2=0,
        event_count_2=0,
    )
