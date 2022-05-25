from __future__ import annotations

from datetime import datetime
from typing import Any, List, cast

import mitzu.adapters.generic_adapter as GA
import mitzu.common.model as M
import pandas as pd
import sqlalchemy as SA
import sqlalchemy_trino.datatype as SA_T
from mitzu.adapters.helper import dataframe_str_to_datetime, pdf_string_array_to_array
from mitzu.adapters.sqlalchemy_adapter import FieldReference, SQLAlchemyAdapter


class TrinoAdapter(SQLAlchemyAdapter):
    def __init__(self, source: M.EventDataSource):
        super().__init__(source)

    def get_conversion_df(self, metric: M.ConversionMetric) -> pd.DataFrame:
        df = super().get_conversion_df(metric)
        df = dataframe_str_to_datetime(df, GA.DATETIME_COL)
        df[GA.CVR_COL] = df[GA.CVR_COL].astype(float)
        return df

    def get_segmentation_df(self, metric: M.SegmentationMetric) -> pd.DataFrame:
        df = super().get_segmentation_df(metric)
        df = dataframe_str_to_datetime(df, GA.DATETIME_COL)
        return df

    def execute_query(self, query: Any) -> pd.DataFrame:
        if type(query) != str:
            query = str(query.compile(compile_kwargs={"literal_binds": True}))
        return super().execute_query(query=query)

    def map_type(self, sa_type: Any) -> M.DataType:
        if isinstance(sa_type, SA_T.ROW):
            return M.DataType.STRUCT

        return super().map_type(sa_type)

    def _parse_complex_type(
        self, sa_type: Any, name: str, event_data_table: M.EventDataTable, path: str
    ) -> M.Field:
        if isinstance(sa_type, SA_T.ROW):
            row: SA_T.ROW = cast(SA_T.ROW, sa_type)
            sub_fields: List[M.Field] = []
            for n, st in row.attr_types:
                next_path = f"{path}.{n}"
                if next_path in event_data_table.ignored_fields:
                    continue
                sf = self._parse_complex_type(
                    sa_type=st,
                    name=n,
                    event_data_table=event_data_table,
                    path=next_path,
                )
                if sf._type == M.DataType and (
                    sf._sub_fields is None or len(sf._sub_fields) == 0
                ):
                    continue
                sub_fields.append(sf)
            return M.Field(
                _name=name, _type=M.DataType.STRUCT, _sub_fields=tuple(sub_fields)
            )
        else:
            return M.Field(_name=name, _type=self.map_type(sa_type))

    def _get_column_values_df(
        self,
        event_data_table: M.EventDataTable,
        fields: List[M.Field],
        event_specific: bool,
        start_date: datetime,
        end_date: datetime,
    ) -> pd.DataFrame:
        df = super()._get_column_values_df(
            event_data_table=event_data_table,
            fields=fields,
            event_specific=event_specific,
            start_date=start_date,
            end_date=end_date,
        )
        return pdf_string_array_to_array(df)

    def _correct_timestamp(self, dt: datetime) -> Any:
        return SA.text(f"timestamp '{dt}'")

    def _get_last_event_times_pdf(self) -> pd.DataFrame:
        pdf = super()._get_last_event_times_pdf()
        return dataframe_str_to_datetime(pdf, GA.DATETIME_COL)

    def _get_datetime_interval(
        self, field_ref: FieldReference, timewindow: M.TimeWindow
    ) -> Any:
        return SA.func.date_add(
            timewindow.period.name.lower(),
            timewindow.value,
            field_ref,
        )
