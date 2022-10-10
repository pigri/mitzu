from freezegun import freeze_time
from mitzu.samples import get_simple_discovered_project


@freeze_time("2022-10-10")
def test_samples_generator():
    dp = get_simple_discovered_project()
    m = dp.create_notebook_class_model()

    df = (
        (m.page_visit >> m.purchase)
        .config(conv_window="1 week", time_group="week")
        .get_df()
    )

    assert 5 == df.shape[0]
