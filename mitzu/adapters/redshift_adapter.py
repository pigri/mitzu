from __future__ import annotations

from typing import Any

import mitzu.model as M
import pandas as pd
import mitzu.adapters.sqlalchemy_adapter as SA
from mitzu.adapters.postgresql_adapter import PostgresqlAdapter
import mitzu.adapters.generic_adapter as GA
from typing import List, Optional
import mitzu.helper as H
from sqlalchemy import select, literal, distinct
import sqlalchemy.sql.expression as EXP

VALUES_COL_NAME = "values"


class RedshiftAdapter(PostgresqlAdapter):
    def __init__(self, project: M.Project):
        super().__init__(project)

    def execute_query(self, query: Any) -> pd.DataFrame:
        if type(query) != str:

            query = str(query.compile(compile_kwargs={"literal_binds": True}))
            query = query.replace(
                "%", "%%"
            )  # bugfix for redshift, which has string formatting
        return super().execute_query(query=query)

    def _get_conv_aggregation(
        self, metric: M.Metric, cte: EXP.CTE, first_cte: EXP.CTE
    ) -> Any:
        if metric._agg_type == M.AggType.PERCENTILE_TIME_TO_CONV:
            raise NotImplementedError(
                "Percentile calculation is not supported at the moment."
            )
        return super()._get_conv_aggregation(metric, cte, first_cte)

    def _get_column_values_df(
        self,
        event_data_table: M.EventDataTable,
        fields: List[M.Field],
        event_specific: bool,
    ) -> pd.DataFrame:
        # Redshift doesn't support ListAgg and ArrayAgg properly.
        # So the whole process needs to be rethought.
        # We discover with select distinct event_name, column_name_x from table.
        # We iterate through all columns in a loop. Also we use a window function to
        # pick only a sample of rows. We can't use group by at all for
        # discovering event field values.

        if event_data_table.discovery_settings is None:
            raise ValueError("Missing discovery settings")

        event_specific_str = "event specific" if event_specific else "generic"
        H.LOGGER.debug(f"Discovering {event_specific_str} field enums")

        cte = SA.aliased(
            self._get_dataset_discovery_cte(event_data_table),
            alias=SA.SAMPLED_SOURCE_CTE_NAME,
            name=SA.SAMPLED_SOURCE_CTE_NAME,
        )
        event_name_select_field = (
            self.get_event_name_field(event_data_table, cte).label(
                GA.EVENT_NAME_ALIAS_COL
            )
            if event_specific
            else literal(M.ANY_EVENT_NAME).label(GA.EVENT_NAME_ALIAS_COL)
        )
        res_df: Optional[pd.DataFrame] = None
        for f in fields:
            query = select(
                columns=[
                    distinct(
                        event_name_select_field,
                    ),
                    self.get_field_reference(f, sa_table=cte).label(VALUES_COL_NAME),
                ]
            )
            df = self.execute_query(query)
            groupped = (
                df.groupby(df[GA.EVENT_NAME_ALIAS_COL])[VALUES_COL_NAME]
                .apply(
                    lambda val: list(val)
                    if (
                        len(val)
                        < event_data_table.discovery_settings.max_enum_cardinality
                    )
                    else None
                )
                .reset_index()
            )
            groupped = groupped.rename(columns={VALUES_COL_NAME: f._get_name()})
            if res_df is None:
                res_df = groupped
            else:
                res_df = res_df.merge(
                    groupped,
                    left_on=GA.EVENT_NAME_ALIAS_COL,
                    right_on=GA.EVENT_NAME_ALIAS_COL,
                )
        if res_df is None:
            return pd.DataFrame()
        return res_df.set_index(GA.EVENT_NAME_ALIAS_COL)

    def get_retention_df(self, metric: M.RetentionMetric) -> pd.DataFrame:
        res_df = super().get_retention_df(metric)
        res_df[GA.AGG_VALUE_COL] = res_df[GA.AGG_VALUE_COL].astype(float)
        return res_df
