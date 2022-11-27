import os

from mitzu.samples import get_sample_discovered_project

WD = os.path.dirname(os.path.abspath(__file__))


def test_samples_generator():
    dp = get_sample_discovered_project()
    m = dp.create_notebook_class_model()

    df = (
        (m.page_visit >> m.checkout)
        .config(conv_window="1 week", time_group="week")
        .get_df()
    )

    assert 5 == df.shape[0]
