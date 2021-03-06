import application
import string
import boto3
import os
from boto3.dynamodb.conditions import Key, Attr


def CreateTable(tableName):
    dynamodb = boto3.client('dynamodb', region_name='us-west-2')
    try:
        DeleteDynamoTable(tableName)
        print("Table existed, deleted prior to reloading")
    except Exception as e:
        print('Query error in DynamoUtils.createTable: ', e)
    dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
    try:
        table = dynamodb.create_table(
            TableName= tableName,
            KeySchema=[
            {
                'AttributeName': 'LastName',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'FirstName',
                'KeyType': 'RANGE'
            },
            ], AttributeDefinitions=[
            {
                'AttributeName': 'LastName',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'FirstName',
                'AttributeType': 'S'
            },
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 10,
                'WriteCapacityUnits': 10  
            }
        )
        table.meta.client.get_waiter('table_exists').wait(TableName=tableName)
        return tableName
    except Exception as e:
        print("Creating db table, ignoring error: ", e)
        return(e)


# locaFileName = 'input.txt'
def InputLocalFileDataToDynamoDB(cleanedDataList, currTableName):
    listOfData = cleanedDataList.split('\n')
    try:
        dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
        table = dynamodb.Table(currTableName)
    except Exception as e:
        print("Open dynamo table error: ", e)
        return(False)
    try:
        for line in listOfData:
            # currentLine = line[:-2].split(" ")
            currentLine = line.split(" ")
            if len(currentLine) > 2:
                lastName = currentLine[0]
                firstName = currentLine[1]
                memberData = " ".join(currentLine[2:])
                try:
                    table.put_item(
                        Item={
                            'LastName': lastName,
                            'FirstName': firstName,
                            'MemberData': memberData,        
                            }
                    )
                except Exception as e:
                    print("Input data error: ", e)
                    return(False)
    except Exception as e:
        print("Parse file data error: ", e)
        return(False)
    return(True)


def QueryDynamodb(currTableName, lastName, firstName):
    try:
        dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
        table = dynamodb.Table(currTableName)
    except Exception as e:
        print("Open dynamo table error: ", e)
        return(['First, click "Load" to load data into database'])
    if lastName and firstName:
        try:
            response = table.get_item(
                Key={
                    'LastName': lastName,
                    'FirstName': firstName,
                }
            )
        except Exception as e:
            print("get item error; last, first: ", firstName, e)
            return(['Item not in database.'])
        returnString = [response['Item']['FirstName'] + " " + response['Item']['LastName'] + " " + response['Item']['MemberData'][:-1]]
        return returnString
    elif lastName:
        try:
            response = table.query(
                KeyConditionExpression=Key('LastName').eq(lastName)
            )
        except Exception as e:
            print("get item error; last: ", e)
            return(['Item not in database.'])
        # print(firstName)
        if response['Count'] == 0:
            return(['Item not in database.'])
        dataString = [[item['FirstName'], item['LastName'], item['MemberData']] for item in response['Items']]
        returnStringList = [' '.join(dataString[i]) for i in range(len(dataString))]
        returnString = ' '.join(returnStringList)

        return returnStringList
    elif firstName:
        try:
            response = table.scan(
                FilterExpression=Attr('FirstName').begins_with(firstName)
            )
        except Exception as e:
            print("get item error; last: ", e)
            return(['Item not in database.'])

        dataString = [[item['FirstName'], item['LastName'], item['MemberData']] for item in response['Items']]
        if len(dataString) == 0:
            return (['Item not in database.'])
        returnStringList = [' '.join(dataString[i]) for i in range(len(dataString))]
        returnString = ' '.join(returnStringList)

        return returnStringList



# parses data lines to remove white space, leaving only last, first, and attributes
def BuildDataMemberAttributes(inputLine):
    inputAttributes = []

    # this is remove white space
    try:
        for i in range(len(inputLine)):
            if len(inputLine[i]) > 0:
                inputAttributes.append(inputLine[i])
        
        # this is to remove ill-formed attributes
            for item in inputAttributes[2:]:
                # this is to avoid attempting to input white space, like " "
                if "=" not in item:
                    inputAttributes.remove(item)
        return " ".join(inputAttributes)
    except Exception as e:
        print("Error from BuildDataMemeberAttributes: ", e)



def DeleteDynamoTable(tableName):
    try:
        dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
        table = dynamodb.Table(tableName)
        table.delete()
        return("Delete table success.")
    except:
        return(False)