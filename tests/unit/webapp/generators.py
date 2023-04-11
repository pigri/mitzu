import hypothesis.strategies as st
import mitzu.model as M
import mitzu.webapp.model as WM


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
        lookback_window=M.TimeWindow(
            draw(st.integers(min_value=1, max_value=10)),
            draw(st.sampled_from(M.TimeGroup)),
        ),
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
            st.one_of(st.none(), st.lists(field_name(), min_size=1, max_size=10))
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
            st.one_of(st.none(), st.lists(field_name(), min_size=1, max_size=10))
        ),
        event_specific_fields=draw(
            st.one_of(st.none(), st.lists(field_name(), min_size=1, max_size=10))
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
        event_data_tables=draw(st.lists(event_data_table(), min_size=0, max_size=10)),
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
