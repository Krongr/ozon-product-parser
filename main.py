from sqlalchemy import exc
from concurrent.futures import ThreadPoolExecutor

from db_client import DbClient
from ozon_parser import OzonProductParcer
import app_logger


logger = app_logger.create_logger(__name__)

# DB settings:
TYPE= ''
NAME= ''
HOST= ''
PORT= ''
USER= ''
PASSWORD= ''

def run_parser(parser:OzonProductParcer):
    _id = parser.client_id

    try:
        product_ids = parser.collect_product_ids()
        logger.info(f'Product IDs collected ({_id})')
        for i in range(0, len(product_ids), 50):
            _queries = []
            product_cards = parser.collect_product_cards(product_ids[i:i+50])
            logger.info(f'Product cards ({i}-{i+50}) collected ({_id})')

            for _card in product_cards:
                _attribute_queries = parser.create_product_card_queries(_card)
                _queries += _attribute_queries
            logger.info(f'Queries creation completed ({_id})')

            parser.db_client.execute_queries(_queries)
            logger.info(f'Queries execution completed ({_id})')

            parser.db_client.remove_duplicates('product_attr', 'db_i')
            logger.info(f'Duplicates removed ({_id})')
    except exc.SQLAlchemyError:
        logger.exception(f'DB communication error ({_id})')
    except Exception:
        logger.exception(f'Unexpected error ({_id})')


if __name__ == "__main__":
    logger.info(f'Script started')
    db_client = DbClient(TYPE, NAME, HOST, PORT, USER, PASSWORD)

    try:
        credentials = db_client.get_credentials(mp_id=1)
    except exc.SQLAlchemyError:
        logger.exception('Getting credentials failed')

    parsers = []
    for _entry in credentials:
        parsers.append(OzonProductParcer(
            _entry['client_id'],
            _entry['api_key'],
            db_client,
        ))

    with ThreadPoolExecutor(4) as executor:
        executor.map(run_parser, parsers)
    
    logger.info(f'Script completed')
