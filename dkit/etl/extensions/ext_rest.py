from .. import source, DEFAULT_LOG_TRIGGER
from ...exceptions import CkitETLException

import requests


class RESTSource(source.AbstractSource):

    def __init__(self, url, parameters, headers=None, log_trigger=DEFAULT_LOG_TRIGGER):
        super().__init__(log_trigger=log_trigger)
        self.url = url
        self.headers = {}
        self.parameters = parameters
        self.requests = requests
        self._set_content_type_json()

    def _set_content_type_json(self):
        """content type is json"""
        if self.headers is None:
            self.headers = {}
        self.headers["Content-Type"] = "application/json"

    def request_get(self, parameters):
        """
        perform request

        useful for debugging the response
        """
        return self.requests.get(
            self.url,
            params=parameters,
            headers=self.headers
        )

    def __iter__(self):
        response = self.request_get(self.parameters)
        if response.status_code == 200:
            yield from response.json()
        else:
            raise CkitETLException("Status code {}".format(response.status_code))
