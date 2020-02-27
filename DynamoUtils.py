import application
import string
import boto3


def CreateTable():
    dynamodb = boto3.resource('dynamodb')
    try:
        table = dynamodb.create_table(
            TableName='DataMembers',
            KeySchema=[
            {
                'AttributeName': 'LastName',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'FirstName',
                'KeyType': 'RANGE'
            }
        ], AttributeDefinitions=[
            {
                'AttributeName': 'memberData',
                'AttributeType': 'L'
            }
        ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
    except Exception as e:
        if e.response['Error']['Code'] != 'ResourceInUseException':
            raise e

    table.meta.client.get_waiter('table_exists').wait(TableName='DataMembers')

    return dynamodb

# currTable is the dynamodb table to input data to
def InputDynamoData(lastName, firstName, attributes, currTable):

    inputAttributes = []
    for item in range(attributes):
        if "=" in item:
            inputAttributes.append(item.replace('=',':'))
            # input last, first, inputAttributes
    currTable.put_item(
        Item={
            'LastName': lastName,
            'FirstName': firstName,
            'memberData': inputAttributes
        }
    )