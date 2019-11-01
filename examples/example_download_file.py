"""
Sample usage of download_file method.
"""
from dkit.utilities.network_helper import download_file

if __name__ == '__main__':
    url = "https://www.python.org/static/img/python-logo.png"
    status, reason, filename = download_file(url, binary_mode=True)
    print(status, reason, filename)
