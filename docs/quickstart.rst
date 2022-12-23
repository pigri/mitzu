Quick Start
===========

In this section, we will describe how to start with ``Mitzu`` on your local machine. Skip this section if you rather see ``Mitzu`` in a prepared notebook or webapp. Otherwise get ready and fire up your own data-science `notebook <https://jupyter.org/>`_.

.. image:: https://raw.githubusercontent.com/mitzu-io/mitzu/main/resources/mitzu_notebook_hero.gif
   :alt: Notebook example


Install the Mitzu python library
--------------------------------

Mitzu can be installed as a pip package.

.. code-block:: shell

   pip install mitzu

Reading The Sample Dataset
--------------------------

The simplest way to get started with `Mitzu` is in a data-science notebook. In your notebook read the sample user behavior dataset.
Mitzu can discover your tables in a data warehouse or data lake. For the sake of simplicity we provide you an in-memory `sqlite <https://www.sqlite.org/index.html>`_ based table that contains

.. code-block:: python

   import mitzu.samples as smp

   dp = smp.get_sample_discovered_project()
   m = dp.create_notebook_class_model()


Segmentation
------------

The following command visualizes the ``count of unique users`` over time who did ``page visit`` action in the last ``30 days``.

.. code-block:: python

   m.page_visit


.. image:: https://raw.githubusercontent.com/mitzu-io/mitzu/main/resources/segmentation.png
   :alt: segmentation metric

In the example above ``m.page_visit`` refers to a ``user event segment`` for which the notebook representation is a ``segmentation chart``.
If this sounds unfamiliar, don't worry! Later we will explain you everything.

Funnels
-------

You can create a ``funnel chart`` by placing the ``>>`` operator between two ``user event segments``.

.. code-block:: python

   m.page_visit >> m.checkout


This will visualize the ``conversion rate`` of users that first did ``page_visit`` action and then did ``checkout`` within a day in the last 30 days.

Filtering
---------

You can apply filters to ``user event segment`` the following way:

.. code-block:: python

   m.page_visit.user_country_code.is_us >> m.checkout

   # You can achieve the same filter with:
   # (m.page_visit.user_country_code == 'us')
   #
   # you can also apply >, >=, <, <=, !=, operators.


With this syntax we have narrowed down our ``page visit`` ``user event segment`` to page visits from the ``US``.
Stacking filters is possible with the ``&`` (and) and ``|`` (or) operators.

.. code-block:: python

   m.page_visit.user_country_code.is_us & m.page_visit.acquisition_campaign.is_organic

   # if using the comparison operators, make sure you put the user event segments in parenthesis.
   # (m.page_visit.user_country_code == 'us') & (m.page_visit.acquisition_campaign == 'organic')


Apply multi value filtering with the ``any_of`` or ``none_of`` functions:

.. code-block:: python

   m.page_visit.user_country_code.any_of('us', 'cn', 'de')

   # m.page_visit.user_country_code.none_of('us', 'cn', 'de')


Of course you can apply filters on every ``user event segment`` in a funnel.

.. code-block:: python

   m.add_to_cart >> (m.checkout.cost_usd <= 1000)


Metrics Configuration
---------------------

To any funnel or segmentation you can apply the config method. Where you can define the parameters of the metric.

.. code-block:: python

   m.page_visit.config(
      start_dt="2021-08-01",
      end_dt="2021-09-01",
      group_by=m.page_visit.domain,
      time_group='total',
   )

- ``start_dt`` should be an iso datetime string, or python datetime, where the metric should start.
- ``end_dt`` should be an iso datetime string, or python datetime, where the metric should end.
- ``group_by`` is a property that you can refer to from the notebook class model.
- ``time_group`` is the time granularity of the query for which the possible values are: ``hour``, ``day``, ``week``, ``month``, ``year``, ``total``

Funnels have an extra configuration parameter ``conv_window``, this has the following format: ``<VAL> <TIME WINDOW>``, where ``VAL`` is a positive integer.

.. code-block:: python

   (m.page_visit >> m.checkout).config(
      start_dt="2021-08-01",
      end_dt="2021-09-01",
      group_by=m.page_visit.domain,
      time_group='total',
      conv_window='1 day',
   )


SQL Generator
-------------

For any metric you can print out the SQL code that ``Mitzu`` generates.
This you can do by calling the ``.print_sql()`` method.

.. code-block:: python

   (m.page_visit >> m.checkout).config(
      start_dt="2021-08-01",
      end_dt="2021-09-01",
      group_by=m.page_visit.domain,
      time_group='total',
      conv_window='1 day',
   ).print_sql()


.. image:: https://raw.githubusercontent.com/mitzu-io/mitzu/main/resources/print_sql.png
   :alt: webapp example

Pandas DataFrames
-----------------

Similarly you can access the results in the form of a `Pandas <https://pandas.pydata.org/>`_ DataFrame with the method ``.get_df()``

.. code-block:: python

   (m.page_visit >> m.checkout).config(
      start_dt="2021-08-01",
      end_dt="2021-09-01",
      group_by=m.page_visit.domain,
      time_group='total',
      conv_window='1 day',
   ).get_df()


Notebook Dashboards
-------------------

You can also visualize the webapp in a Jupyter Notebook:

.. code-block:: python

   import mitzu.samples as smp

   dp = smp.get_sample_discovered_project()
   dp.notebook_dashboard()


.. image:: https://raw.githubusercontent.com/mitzu-io/mitzu/main/resources/dash_notebook.png
   :alt: dash

Usage In Notebooks
------------------

- `Example notebook <https://deepnote.com/@istvan-meszaros/Mitzu-Introduction-af037f5a-2184-494d-9362-6f4c69b5eedc>`_
- `Documentation <https://mitzu.io/documentation/notebook>`_

Webapp
------

Mitzu can run as a standalone webapp or embedded inside a notebook.

Trying out locally:

.. code-block:: shell

   docker run -p 8082:8082 imeszaros/mitzu-webapp


- `Example webapp <https://app.mitzu.io>`_
- `Webapp documentation <https://mitzu.io/documentation/webapp>`_
