import mitzu.model as M
import mitzu.adapters.postgresql_adapter as PA
from typing import cast
from datetime import datetime


def test_complex_types_postgres():
    project = M.Project(
        project_name="complex_types_project",
        connection=M.Connection(
            connection_name="Sample project",
            connection_type=M.ConnectionType.POSTGRESQL,
            host="localhost",
            secret_resolver=M.ConstSecretResolver("test"),
            user_name="test",
            catalog="postgres",
        ),
        event_data_tables=[
            M.EventDataTable.create(
                table_name="complex_table",
                schema="public",
                event_name_field="event",
                event_time_field="event_time",
                user_id_field="uuid_text",
            )
        ],
        webapp_settings=M.WebappSettings(
            end_date_config=M.WebappEndDateConfig.CUSTOM_DATE,
            custom_end_date=datetime(2024, 1, 1),
        ),
        discovery_settings=M.DiscoverySettings(lookback_days=1000),
    )

    adapter = cast(PA.PostgresqlAdapter, project.get_adapter())

    # The following SQL creates a view that contains random
    # jsonb and json types for testing
    adapter.get_engine().execute(
        """
            CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
            DROP VIEW IF EXISTS complex_table;
            CREATE VIEW complex_table AS 
            select 
                timestamp '2023-01-01 20:00:00' + random() * 
                    (timestamp '2023-01-01 20:00:00' - timestamp '2024-01-01 10:00:00') as event_time, 
                (array['event_1','event_2','event_3'])[floor(random()*3)+1] as event,
                (array['a','b','c'])[floor(random()*3)+1] as root_property, 
                uuid_generate_v1() as uuid,
                ('{"a":' || True || ', "b":2, "c":"text"}')::jsonb as complex_field,
                jsonb_object(ARRAY['a','b','c'],
                    ARRAY[(array['a','b','c'])[floor(random()*3)+1],
                    (array['x','y','z'])[floor(random()*3)+1],
                    (array['aaa','bbb','ccc'])[floor(random()*3)+1]
                    ]) as jsonb_properties,
                json_object(ARRAY['a','b','c'],
                    ARRAY[(array['a','b','c'])[floor(random()*3)+1],
                    (array['x','y','z'])[floor(random()*3)+1],
                    (array['aaa','bbb','ccc'])[floor(random()*3)+1]
                    ]) as json_properties,
                cast(uuid_generate_v1() as text) as uuid_text
            from (SELECT a.n from generate_series(1, 1000) as a(n)) as vals
    """
    )

    dp = project.discover_project()

    m = dp.create_notebook_class_model()
    metric = (
        m.event_1.jsonb_properties.a.is_a | m.event_2.json_properties.b.is_z
    ).group_by(m.event_1.jsonb_properties.b)
    res = metric.get_sql()
    print(res)
    assert (
        """WITH anon_2 AS
  (SELECT t1.uuid_text AS _cte_user_id,
          t1.event_time AS _cte_datetime,
          (t1.jsonb_properties ->> 'b') AS _cte_group
   FROM public.complex_table AS t1
   WHERE t1.event = 'event_1'
     AND t1.event_time >= '2023-12-02 00:00:00'
     AND t1.event_time <= '2024-01-01 00:00:00'
     AND TRUE
     AND (t1.jsonb_properties ->> 'a') = 'a'
     OR t1.event = 'event_2'
     AND t1.event_time >= '2023-12-02 00:00:00'
     AND t1.event_time <= '2024-01-01 00:00:00'
     AND TRUE
     AND (t1.json_properties ->> 'b') = 'z')
SELECT date_trunc('DAY', anon_1._cte_datetime) AS _datetime,
       anon_1._cte_group AS _group,
       count(DISTINCT anon_1._cte_user_id) AS _agg_value
FROM anon_2 AS anon_1
GROUP BY 1,
         2"""
        == res
    )

    res = metric.get_df()

    assert res is not None

    metric = (
        m.event_1.complex_field.a.is_true
        | (m.event_2.complex_field.b < 10)
        | m.event_2.complex_field.c.like("text")
    ).group_by(m.event_1.complex_field.a)
    res = metric.get_sql()

    # Here I am testing the various different types that a JSON can handle (bool, number, string)
    # bool needs to be casted to `::boolean`
    # number to ::float
    # string needs to have the operator ->> for value access in json
    assert (
        res
        == """WITH anon_2 AS
  (SELECT t1.uuid_text AS _cte_user_id,
          t1.event_time AS _cte_datetime,
          (t1.complex_field -> 'a')::boolean AS _cte_group
   FROM public.complex_table AS t1
   WHERE t1.event = 'event_1'
     AND t1.event_time >= '2023-12-02 00:00:00'
     AND t1.event_time <= '2024-01-01 00:00:00'
     AND TRUE
     AND (t1.complex_field -> 'a')::boolean = TRUE
     OR t1.event = 'event_2'
     AND t1.event_time >= '2023-12-02 00:00:00'
     AND t1.event_time <= '2024-01-01 00:00:00'
     AND TRUE
     AND (t1.complex_field -> 'b')::numeric < 10
     OR t1.event = 'event_2'
     AND t1.event_time >= '2023-12-02 00:00:00'
     AND t1.event_time <= '2024-01-01 00:00:00'
     AND TRUE
     AND (t1.complex_field ->> 'c') LIKE 'text')
SELECT date_trunc('DAY', anon_1._cte_datetime) AS _datetime,
       anon_1._cte_group AS _group,
       count(DISTINCT anon_1._cte_user_id) AS _agg_value
FROM anon_2 AS anon_1
GROUP BY 1,
         2"""
    )

    res = metric.get_df()
    assert res is not None
