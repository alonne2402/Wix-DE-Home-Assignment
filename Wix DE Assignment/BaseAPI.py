import requests
import logging
from retry import retry


class ServerErrorException(Exception):
    pass


class ClientErrorException(Exception):
    pass

class BaseAPI:
    def __init__(self, api_key=None, base_rul=None, end_point=None):
        self.api_key = api_key
        self.base_rul = base_rul
        self.end_point = end_point

    # build API URL
    def _build_url(self):
        if self.end_point is None:
            return  self.base_rul
        else:
            return f"{self.base_rul}/{self.end_point}"

    # get response from the API
    def _send_request(self, url: str, params=None) -> requests.Response:
        logging.info(f'Sending request to the next url - {url} using get method')
        response = requests.get(url, params=params, timeout=10)
        return response

    # get API response and handle errors
    @retry(ServerErrorException, tries=3, delay=2)
    def _handle_request(self, response):
        try:
            if response.status_code == 200:
                return response.json()
            elif response.status_code >= 400 and response.status_code <= 499:
                logging.error(
                    f"Client error occurred. Status Code: {response.status_code}. Response Content: {response.content}"
                )
                raise ClientErrorException(f"Client error occurred: {response.content}")
            elif response.status_code >= 500:
                logging.error(
                    f"Server error occurred. Status Code: {response.status_code}. Response Content: {response.content}"
                )
                raise ServerErrorException(f"Server error occurred: {response.content}")
            else:
                raise Exception(f"Unexpected error occurred: {response.content}")
        except requests.exceptions.Timeout:
            logging.error("The request timed out.")
            raise TimeoutError("The request to the server timed out.")
        except requests.exceptions.RequestException as e:
            logging.error(f"An error occurred during the request: {e}")
            raise Exception(f"An error occurred during the request: {e}")

    # build the request
    def _build_request(self, url: str, params=None):
        try:
            response = self._send_request(url,params)
            return self._handle_request(response)
        except (ServerErrorException, ClientErrorException, Exception) as e:
            raise e