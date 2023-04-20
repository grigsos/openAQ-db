
# lamba-interview
 
Change the environmental to your option

 - TABLE_NAME: str = os.environ.get('dynamoDB_TABLE_NAME')
 - REGION_NAME: str = os.environ.get('REGION_NAME')
 - COUNTRY_ID: str = os.environ.get('COUNTRY_ID')
 - HOURS_THRESHOLD: int = int(os.environ.get('HOURS_THRESHOLD', '5'))
 - MUTEX_KEY: int = int(os.environ.get('MUTEX_KEY'))
 - PARTITION_KEY: str = os.environ.get('PARTITION_KEY')


But first initialise dynamoDB table in your region with partition key that is a number
