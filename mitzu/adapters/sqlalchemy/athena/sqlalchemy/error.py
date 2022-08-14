# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


class AthenaQueryError(Exception):
    def __init__(self, error, query_id=None):
        self._error = error
        self._query_id = query_id

    @property
    def error_code(self):
        return self._error.get("errorCode", None)

    @property
    def error_name(self):
        return self._error.get("errorName", None)

    @property
    def error_type(self):
        return self._error.get("errorType", None)

    @property
    def error_exception(self):
        return self.failure_info.get("type", None) if self.failure_info else None

    @property
    def failure_info(self):
        return self._error.get("failureInfo", None)

    @property
    def message(self):
        return self._error.get("message", "Databricks did no return an error message")

    @property
    def error_location(self):
        location = self._error["errorLocation"]
        return (location["lineNumber"], location["columnNumber"])

    @property
    def query_id(self):
        return self._query_id

    def __repr__(self):
        return '{}(type={}, name={}, message="{}", query_id={})'.format(
            self.__class__.__name__,
            self.error_type,
            self.error_name,
            self.message,
            self.query_id,
        )

    def __str__(self):
        return repr(self)
