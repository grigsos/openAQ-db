import boto3
from typing import Optional

from constants import TABLE_NAME, MUTEX_KEY , PARTITION_KEY

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(TABLE_NAME)


def get_mutex() -> bool:
    try:
        table.put_item(
            Item={
                PARTITION_KEY: MUTEX_KEY,
                "locked": True
            },
            ConditionExpression="attribute_not_exists(locationID)"
        )
        return True
    except dynamodb.meta.client.exceptions.ConditionalCheckFailedException:
        return False


def release_mutex() -> Optional[bool]:
    response = table.delete_item(
        Key={
            PARTITION_KEY: MUTEX_KEY
        },
        ConditionExpression="attribute_exists({}) AND locked = :locked".format(PARTITION_KEY),
        ExpressionAttributeValues={
            ":locked": True
        },
        ReturnValues="ALL_OLD"
    )
    if "Attributes" in response:
        return True
    else:
        return None
