import requests
import json


class APIError(Exception):
    pass

class ConnectionError(APIError):
    def __init__(self, url):
        super().__init__(f'Failed to connect {url}')

class BadResponse(APIError):
    def __init__(self, response):
        super().__init__(f'{response.json()}')


class OzonApi():
    def __init__(self, client_id, api_key):
        self.api_url = 'https://api-seller.ozon.ru'
        self.headers = {
            'Content-Type': 'application/json',
            'Client-Id': f'{client_id}',
            'Api-key': f'{api_key}',
        }
    
    def request_product_list(self, last_id='', limit=1000):
        """Returns response with a list of customer's (Client-Id) products
        placed on Ozon. 'last_id' can be used to iterate over large lists.
        """
        _url = f'{self.api_url}/v2/product/list'
        _data = {
            'last_id': last_id,
            'limit': limit,
        }
        try:
            response = requests.post(
                url=_url,
                headers=self.headers,
                data=json.dumps(_data),
            )
        except (
            requests.exceptions.InvalidSchema,
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
        ) as error:
            raise ConnectionError(_url) from error

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as error:
            raise BadResponse(response) from error

        return response

    def request_product_attributes(self, product_ids:list, last_id='', 
                               limit=1000):
        """Returns response with a list of dictionaries with with
        product attributes.
        Even though it is suggested to use 'last_id' for
        iterating over large lists, method of application is not clear.
        For this reason, the use of lists longer than 50 values
        is not recommended.
        """
        _url = f'{self.api_url}/v3/products/info/attributes'
        _data = {
            "filter": {
                "product_id": product_ids,
                "visibility": "ALL",
            },
            'last_id': last_id,
            'limit': limit,
        }
        try:
            response = requests.post(
                url=_url,
                headers=self.headers,
                data=json.dumps(_data),
            )
        except (
            requests.exceptions.InvalidSchema,
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
        ) as error:
            raise ConnectionError(_url) from error

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as error:
            raise BadResponse(response) from error

        return response

    def request_product_description(self, product_id:int):
        """Returns a dictionary containing product description.
        """
        _url = f'{self.api_url}/v1/product/info/description'
        _data = {
            'product_id': product_id,
        }
        try:
            response = requests.post(
                url=_url,
                headers=self.headers,
                data=json.dumps(_data),
            )
        except (
            requests.exceptions.InvalidSchema,
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
        ) as error:
            raise ConnectionError(_url) from error

        try:
            response.raise_for_status()
        except Exception as error:
            raise BadResponse(response) from error

        return response
