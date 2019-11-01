from flask import Flask, json, request
import unittest
# import requests
import time
from multiprocessing import Process
import sys; sys.path.insert(0, "..")
from dkit.etl.extensions.ext_rest import RESTSource


SERVER_URL = "http://127.0.0.1:5000"
app = Flask(__name__)


@app.route("/api/v1.0/temperatures/", methods=['GET'])
def api_temperature():
    if request.args["api_key"] == "123":
        return json.dumps([{"temp": 10} for _ in range(25)])
    else:
        return []


class WebServiceThreadingTestCase(unittest.TestCase):
    """A Test case to test the thread safety of a web service."""

    @classmethod
    def setUpClass(cls):

        def start_and_init_server(app):
            app.run(threaded=True)

        cls.app = app
        cls.server_thread = Process(target=start_and_init_server, args=(cls.app, ))
        cls.server_thread.start()
        time.sleep(0.8)

    @classmethod
    def tearDownClass(cls):
        cls.server_thread.terminate()
        cls.server_thread.join()

    def runTest(self):
        """Run the actual threading test."""
        url = SERVER_URL + "/api/v1.0/temperatures/"
        params = {
            "api_key": "123"
        }
        src = list(RESTSource(url, params))
        self.assertEqual(sum(i["temp"] for i in src), 250)


if __name__ == "__main__":
    unittest.main()
