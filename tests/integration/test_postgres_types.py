import mitzu.model as M
import mitzu.adapters.postgresql_adapter as PA
from typing import cast
from datetime import datetime


def test_complex_types_postgres():
    tbl = M.EventDataTable.create(
        table_name="complex_table",
        schema="public",
        event_name_field="event",
        event_time_field="event_time",
        user_id_field="uuid_text",
    )
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
        event_data_tables=[tbl],
        webapp_settings=M.WebappSettings(
            end_date_config=M.WebappEndDateConfig.CUSTOM_DATE,
            custom_end_date=datetime(2023, 1, 5),
        ),
        discovery_settings=M.DiscoverySettings(lookback_days=4),
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
                'event' as event,
                timestamp '2023-01-01 20:00:00' event_time,                 
                uuid_generate_v1() as uuid,
                cast(uuid_generate_v1() as text) as uuid_text,
                '{"a": true, "b":2, "c":"text1", "d": ["1","2","3"], "e": null, "f": {"x":"y"}, "h": 1, "g": true}'::jsonb as complex_field
            union all
            select
                'event' as event,
                timestamp '2023-01-02 20:00:00' event_time,                 
                uuid_generate_v1() as uuid,
                cast(uuid_generate_v1() as text) as uuid_text,
                ('{"a": false, "b":2, "c":"texg2", "d": ["1"], "e": null, "f": {},  "h": "text", "g": null}')::jsonb as complex_field
            union all
            select
                'event' as event,
                timestamp '2023-01-03 20:00:00' event_time,                 
                uuid_generate_v1() as uuid,
                cast(uuid_generate_v1() as text) as uuid_text,
                ('{"a": null, "b":null, "c":"null", "d": null, "e": null, "f": null, "h": true, "g": null}')::jsonb as complex_field
            union all
            select
                'event' as event,
                timestamp '2023-01-04 20:00:00' event_time,                 
                uuid_generate_v1() as uuid,
                cast(uuid_generate_v1() as text) as uuid_text,
                ('{}')::jsonb as complex_field

    """
    )

    dp = project.discover_project()

    fields = dp.definitions[tbl]["event"].get_value_if_exists()._fields
    print([f._field for f in fields])

    m = dp.create_notebook_class_model()
    metric = (
        m.event.complex_field.a.is_true
        | m.event.complex_field.a.is_null
        | (m.event.complex_field.b == 2)
        | m.event.complex_field.b.is_null
        | m.event.complex_field.c.is_text1
        | m.event.complex_field.c.like("text1%")
        | m.event.complex_field.c.is_null
        | m.event.complex_field.h.is_1
    ).group_by(m.event.complex_field.c)
    res = metric.get_sql()

    print(res)
    assert (
        """WITH anon_2 AS
  (SELECT t1.uuid_text AS _cte_user_id,
          t1.event_time AS _cte_datetime,
          (t1.complex_field ->> 'c')::text AS _cte_group
   FROM public.complex_table AS t1
   WHERE t1.event = 'event'
     AND t1.event_time >= '2022-12-06 00:00:00'
     AND t1.event_time <= '2023-01-05 00:00:00'
     AND TRUE
     AND (t1.complex_field ->> 'a')::boolean = TRUE
     OR t1.event = 'event'
     AND t1.event_time >= '2022-12-06 00:00:00'
     AND t1.event_time <= '2023-01-05 00:00:00'
     AND TRUE
     AND (t1.complex_field ->> 'a')::boolean IS NULL
     OR t1.event = 'event'
     AND t1.event_time >= '2022-12-06 00:00:00'
     AND t1.event_time <= '2023-01-05 00:00:00'
     AND TRUE
     AND (t1.complex_field ->> 'b')::numeric = 2
     OR t1.event = 'event'
     AND t1.event_time >= '2022-12-06 00:00:00'
     AND t1.event_time <= '2023-01-05 00:00:00'
     AND TRUE
     AND (t1.complex_field ->> 'b')::numeric IS NULL
     OR t1.event = 'event'
     AND t1.event_time >= '2022-12-06 00:00:00'
     AND t1.event_time <= '2023-01-05 00:00:00'
     AND TRUE
     AND (t1.complex_field ->> 'c')::text = 'text1'
     OR t1.event = 'event'
     AND t1.event_time >= '2022-12-06 00:00:00'
     AND t1.event_time <= '2023-01-05 00:00:00'
     AND TRUE
     AND (t1.complex_field ->> 'c')::text LIKE 'text1%'
     OR t1.event = 'event'
     AND t1.event_time >= '2022-12-06 00:00:00'
     AND t1.event_time <= '2023-01-05 00:00:00'
     AND TRUE
     AND (t1.complex_field ->> 'c')::text IS NULL
     OR t1.event = 'event'
     AND t1.event_time >= '2022-12-06 00:00:00'
     AND t1.event_time <= '2023-01-05 00:00:00'
     AND TRUE
     AND (t1.complex_field ->> 'h')::text = '1')
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
