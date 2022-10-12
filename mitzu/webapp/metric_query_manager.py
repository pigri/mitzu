import traceback
from dataclasses import dataclass
from threading import Thread, current_thread
from typing import Optional

import mitzu.model as M
import pandas as pd
from mitzu.webapp.helper import LOGGER

THREAD_SLEEP = 0.2


@dataclass
class MetricQueryManager:
    __last_error: Optional[Exception] = None
    __last_result: Optional[pd.DataFrame] = None
    __last_metric: Optional[M.Metric] = None
    __thread: Optional[Thread] = None

    def __execute_queries(self):
        if (
            self.__last_metric is not None
            and self.__last_error is None
            and self.__last_result is None
        ):
            try:
                LOGGER.debug("Executing new query")
                res = self.__last_metric.get_df()
                if current_thread() == self.__thread:
                    self.__last_result = res
                    LOGGER.debug("Finished query execution")
                else:
                    LOGGER.debug("Results ignored")
            except Exception as exc:
                if current_thread() == self.__thread:
                    traceback.print_exc()
                    LOGGER.error(f"Query execution failed {exc}")
                    self.__last_error = exc

    def try_get_metric_result(self, metric: M.Metric) -> Optional[pd.DataFrame]:
        if id(metric) == id(self.__last_metric):
            if self.__last_result is not None:
                return self.__last_result
            elif self.__last_error is not None:
                raise self.__last_error
        else:
            self.__last_metric = metric
            self.__last_error = None
            self.__last_result = None
            self.__thread = Thread(target=self.__execute_queries)
            self.__thread.start()
        return None
