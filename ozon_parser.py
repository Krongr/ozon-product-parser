from sqlalchemy import exc

import app_logger
from ozon_api import OzonApi, ConnectionError, BadResponse


logger = app_logger.create_logger(__name__)

class OzonProductParcer(OzonApi):
    def __init__(self, client_id, api_key, db_client):
        super().__init__(client_id, api_key)
        self.client_id = client_id
        self.db_client = db_client

    def collect_product_ids(self)->list:
        """Returns a list of product ids.
        Iterates over large lists of products using 'last_id'.
        """
        product_ids = []
        last_id = ''
        try:
            while True:
                response = self.request_product_list(last_id)
                result = response.json()['result']
                products = result['items']
                if products:
                    for _entry in products:
                            product_ids.append(_entry['product_id'])
                    last_id = result['last_id']
                else:
                    break
        except BadResponse:
            logger.exception('API method error')
        except ConnectionError:
            logger.exception('Connection error')
        except (
            TypeError,
            KeyError,
            AttributeError,
        ):
            logger.exception('JSON parsing error')
        except Exception:
            logger.exception('Unexpected error')
        finally:
            return product_ids

    def get_product_description(self, product_id)->str:
        description = ''
        try:
            response = self.request_product_description(product_id)
            description = response.json()['result']['description']
        except BadResponse:
            logger.exception('API method error')
        except ConnectionError:
            logger.exception('Connection error')
        except (
            TypeError,
            KeyError,
            AttributeError,
        ):
            logger.exception('JSON parsing error')
        except Exception:
            logger.exception('Unexpected error')
        finally:
            return description

    def collect_product_cards(self, product_ids:list):
        """Returns a list of product cards
        containing information about product attributes.
        """
        product_cards = []
        try:
            response = self.request_product_attributes(product_ids)
            result = response.json()['result']       
            for _entry in result:
                description = self.get_product_description(_entry['id'])
                _entry['description'] = description
                product_cards.append(_entry)
        except BadResponse:
            logger.exception('API method error')
        except ConnectionError:
            logger.exception('Connection error')
        except (
            TypeError,
            KeyError,
            AttributeError,
        ):
            logger.exception('JSON parsing error')
        except Exception:
            logger.exception('Unexpected error')
        finally:
            return product_cards

    def create_attribute_query(self, product_id, attribute_id, value,
                               dictionary_value_id='', complex_id=''):
        """Returns a query for adding attribute to the DB"""
        value = str(value).replace("'", "''")
        query = f"""
        INSERT
        INTO product_attr(
            product_id,
            attribute_id,
            value,
            dictionary_value_id,
            complex_id,
            mp_id,
            db_i
        )
        VALUES(
            '{product_id}',
            '{attribute_id}',
            '{value}',
            '{dictionary_value_id}',
            '{complex_id}',
            1,
            '{product_id}{attribute_id}'
        );
        """
        return query.replace(r'%', r'%%')
            
    def create_product_card_queries(self, card:dict):
        """Returns queries for adding all product attributes to the DB."""
        queries = []
        LIST_ATTRIBUTES = (
            'images',
            'images360',
            'pdf_list',
        )
        for _key, _value in card.items():
            try:
                if _key in LIST_ATTRIBUTES:
                    value_list = []
                    for _item in _value:
                        value_list.append(_item['file_name'])
                    _complex_value = '|'.join(value_list)
                    _query = self.create_attribute_query(
                        card['id'],
                        _key,
                        _complex_value,
                    )
                    queries.append(_query)

                elif _key == 'attributes':
                    for _attribute in _value:
                        for _item in _attribute['values']:
                            _query = self.create_attribute_query(
                                card['id'],
                                _attribute['attribute_id'],
                                _item['value'],
                                _item['dictionary_value_id'],
                                _attribute['complex_id'],
                            )
                            queries.append(_query)

                elif _key not in ('id', 'last_id'):
                    _query = self.create_attribute_query(
                        card['id'],
                        _key,
                        _value,
                    )
                    queries.append(_query)
            except (
                TypeError,
                KeyError,
                AttributeError,
            ):
                logger.exception('Product card parsing error')
            except Exception:
                logger.exception('Unexpected error')

        return queries
