import os

TABLE_NAME: str = os.environ.get('dynamoDB_TABLE_NAME')
REGION_NAME: str = os.environ.get('REGION_NAME')
COUNTRY_ID: str = os.environ.get('COUNTRY_ID')
DAYS_THRESHOLD: int = int(os.environ.get('DAYS_THRESHOLD', '5'))

if not all([TABLE_NAME, REGION_NAME, COUNTRY_ID]):
    raise ValueError('One or more of the required environment variables is not defined')
    
#Could be hardcoded but feels unnecessary at current time

LIMIT: int = 1000
PAGE: int = 1
OFFSET: int = 0
RADIUS: int = 1000
ORDER_BY: str = 'lastUpdated'
