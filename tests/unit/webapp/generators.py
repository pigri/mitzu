import base64
import os
from typing import Optional

from hypothesis.extra.pandas import columns, data_frames
import hypothesis.strategies as st
import mitzu.model as M
import mitzu.visualization.common as VC
import mitzu.webapp.model as WM

MAX_EXAMPLES = int(os.getenv("HYPOTHESIS_MAX_EXAMPLES", "10"))


def simple_string():
    return st.text(min_size=2, max_size=10)


def field_name():
    return st.text(min_size=2, max_size=10).filter(lambda x: "." not in x)


def optional_simple_string():
    return st.one_of(st.none(), simple_string())


@st.composite
def secret_resolver(draw):
    return draw(
        st.sampled_from(
            [
                M.PromptSecretResolver(title=draw(simple_string())),
                M.ConstSecretResolver(secret=draw(simple_string())),
                M.EnvVarSecretResolver(
                    variable_name=draw(simple_string().map(lambda x: x.upper()))
                ),
            ]
        )
    )


@st.composite
def time_window(draw):
    return M.TimeWindow(
        draw(st.integers(min_value=1, max_value=10)),
        draw(st.sampled_from(M.TimeGroup)),
    )


@st.composite
def connection(draw):
    return M.Connection(
        connection_name=draw(simple_string()),
        connection_type=draw(st.sampled_from(M.ConnectionType)),
        user_name=draw(optional_simple_string()),
        host=draw(optional_simple_string()),
        port=draw(st.one_of(st.none(), st.integers(min_value=1, max_value=65534))),
        url=draw(optional_simple_string()),
        schema=draw(optional_simple_string()),
        catalog=draw(optional_simple_string()),
        url_params=draw(optional_simple_string()),
        extra_configs=draw(
            st.dictionaries(
                keys=simple_string(),
                values=st.one_of(
                    st.integers(min_value=0, max_value=2000), simple_string()
                ),
                max_size=4,
            )
        ),
        secret_resolver=draw(st.one_of(st.none(), secret_resolver())),
    )


@st.composite
def discovery_settings(draw):
    return M.DiscoverySettings(
        max_enum_cardinality=draw(st.integers(min_value=0, max_value=600)),
        max_map_key_cardinality=draw(st.integers(min_value=500, max_value=2000)),
        end_dt=draw(st.one_of(st.none(), st.datetimes())),
        property_sample_rate=draw(st.integers(min_value=0, max_value=100)),
        lookback_days=draw(st.integers(min_value=2, max_value=30)),
        min_property_sample_size=draw(st.integers(min_value=500, max_value=1500)),
    )


@st.composite
def webapp_settings(draw):
    return M.WebappSettings(
        lookback_window=draw(time_window()),
        auto_refresh_enabled=draw(st.booleans()),
        end_date_config=draw(st.sampled_from(M.WebappEndDateConfig)),
        custom_end_date=draw(st.one_of(st.none(), st.datetimes())),
    )


@st.composite
def single_event_data_table(draw):
    return M.EventDataTable.single_event_table(
        table_name=draw(simple_string()),
        event_time_field=draw(field_name()),
        user_id_field=draw(field_name()),
        schema=draw(optional_simple_string()),
        catalog=draw(optional_simple_string()),
        event_name_alias=draw(optional_simple_string()),
        ignored_fields=draw(
            st.one_of(st.none(), st.lists(field_name(), min_size=1, max_size=5))
        ),
        date_partition_field=draw(st.one_of(st.none(), field_name())),
        description=draw(optional_simple_string()),
        discovery_settings=draw(st.one_of(st.none(), discovery_settings())),
    )


@st.composite
def multi_event_data_table(draw):
    return M.EventDataTable.multi_event_table(
        table_name=draw(simple_string()),
        event_time_field=draw(field_name()),
        user_id_field=draw(field_name()),
        schema=draw(optional_simple_string()),
        catalog=draw(optional_simple_string()),
        event_name_field=draw(st.one_of(st.none(), field_name())),
        ignored_fields=draw(
            st.one_of(st.none(), st.lists(field_name(), min_size=1, max_size=5))
        ),
        event_specific_fields=draw(
            st.one_of(st.none(), st.lists(field_name(), min_size=1, max_size=5))
        ),
        date_partition_field=draw(st.one_of(st.none(), field_name())),
        description=draw(optional_simple_string()),
        discovery_settings=draw(st.one_of(st.none(), discovery_settings())),
    )


@st.composite
def event_data_table(draw):
    return draw(st.one_of(single_event_data_table(), multi_event_data_table()))


@st.composite
def project(draw):
    return M.Project(
        connection=draw(connection()),
        event_data_tables=draw(st.lists(event_data_table(), min_size=0, max_size=5)),
        project_name=draw(simple_string()),
        project_id=draw(optional_simple_string()),
        description=draw(optional_simple_string()),
        discovery_settings=draw(st.one_of(st.none(), discovery_settings())),
        webapp_settings=draw(st.one_of(st.none(), webapp_settings())),
    )


@st.composite
def user(draw):
    return WM.User(
        email=draw(st.emails()),
        password_hash=draw(simple_string()),
        password_salt=draw(simple_string()),
        role=draw(st.sampled_from(WM.Role)),
    )


@st.composite
def chart(draw):
    return VC.SimpleChart(
        title=draw(simple_string()),
        x_axis_label=draw(simple_string()),
        y_axis_label=draw(simple_string()),
        color_label=draw(simple_string()),
        yaxis_ticksuffix=draw(simple_string()),
        hover_mode=draw(simple_string()),
        chart_type=draw(st.sampled_from(M.SimpleChartType)),
        dataframe=draw(
            data_frames(columns=columns(["col"], dtype=int)).filter(
                lambda df: df.size > 0
            )
        ),
        # FIXME: label functions are not generated here
    )


@st.composite
def saved_metric(draw, metric_project: Optional[M.Project] = None):
    if metric_project is None:
        p = draw(project())
    else:
        p = metric_project
    return WM.SavedMetric(
        name=draw(simple_string()),
        chart=draw(chart()),
        image_base64=str(
            base64.urlsafe_b64encode(draw(st.binary(min_size=10, max_size=100)))
        ),
        small_base64=str(
            base64.urlsafe_b64encode(draw(st.binary(min_size=10, max_size=100)))
        ),
        project=p,
        metric_json="{}",  # FIXME: this is good metric to start with
    )


@st.composite
def dashboard_metric(draw, metric_project: Optional[M.Project] = None):
    return WM.DashboardMetric(
        id=draw(simple_string()),
        saved_metric=draw(saved_metric(metric_project)),
        x=draw(st.integers(min_value=1, max_value=3)),
        y=draw(st.integers(min_value=1, max_value=3)),
        width=draw(st.integers(min_value=1, max_value=3)),
        height=draw(st.integers(min_value=1, max_value=3)),
    )


@st.composite
def dashboard(draw):
    metric_project = draw(project())
    return WM.Dashboard(
        name=draw(simple_string()),
        dashboard_metrics=draw(
            st.lists(dashboard_metric(metric_project), min_size=1, max_size=6)
        ),
        created_at=draw(st.datetimes()),
        last_updated_at=draw(st.one_of(st.none(), st.datetimes())),
        owner=draw(optional_simple_string()),
    )
