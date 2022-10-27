import mitzu.model as M
from mitzu.samples.data_ingestion import create_and_ingest_sample_project


def test_postgre_sample_data():
    connection = M.Connection(
        connection_type=M.ConnectionType.POSTGRESQL,
        host="localhost",
        secret_resolver=M.ConstSecretResolver("test"),
        user_name="test",
    )
    project = create_and_ingest_sample_project(
        connection,
        number_of_users=1000,
        event_count=20000,
        overwrite_records=True,
    )

    project.validate()
    dp = project.discover_project()

    m = dp.create_notebook_class_model()

    df = m.page_visit.get_df()
    assert df.shape[0] > 1


def test_mysql_sample_data():
    connection = M.Connection(
        connection_type=M.ConnectionType.POSTGRESQL,
        host="localhost",
        secret_resolver=M.ConstSecretResolver("test"),
        user_name="test",
    )
    project = create_and_ingest_sample_project(
        connection,
        number_of_users=1000,
        event_count=20000,
        overwrite_records=True,
    )

    project.validate()
    dp = project.discover_project()

    m = dp.create_notebook_class_model()

    df = m.page_visit.get_df()
    assert df.shape[0] > 1
