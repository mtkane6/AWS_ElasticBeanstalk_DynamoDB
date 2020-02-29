import application
import string
import boto3
import os


def CreateTable():
    dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
    try:
        table = dynamodb.create_table(
            TableName='0727916mtkuweduDataMemberscss436',
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
        table.meta.client.get_waiter('table_exists').wait(TableName='0727916mtkuweduDataMemberscss436')
        return '0727916mtkuweduDataMemberscss436'
    except Exception as e:
        print("Creating db table, ignoring error: ", e)
        return('0727916mtkuweduDataMemberscss436')


# locaFileName = 'input.txt'
def InputLocalFileDataToDynamoDB(localFileName, currTableName):
    localFile = open(localFileName, 'r')
    try:
        dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
        table = dynamodb.Table(currTableName)
    except Exception as e:
        print("Open dynamo table error: ", e)
        return(False)
    try:
        for line in localFile:
            currentLine = line[:-2].split(" ")
            currentLine = line.split(" ")
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


def QueryDynamodb(currTableName, firstName, lastName):
    try:
        dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
        table = dynamodb.Table(currTableName)
    except Exception as e:
        print("Open dynamo table error: ", e)
        return(False)

    response = table.get_item(
    Key={
        'LastName': lastName,
        'FirstName': firstName,
    }
)
    item = response['Item']
    print(item)
    print(table.item_count)

    # table = dynamodb.Table('0727916mtkuweduDataMemberscss436')
    # table.put_item(
    #     Item={
    #         'LastName': 'Kane',
    #         'FirstName': 'Mitchell',
    #         'MemberData': 'Play=golf',        }
    # )

#     response = table.get_item(
#     Key={
#         'LastName': 'Kane',
#         'FirstName': 'Mitchell',
#     }
# )
#     item = response['Item']
#     print(item)
#     print(table.item_count)
    
    

# currTable is the dynamodb table to input data to
# def InputDynamoData(lastName, firstName, attributes, currTable):

#     # inputAttributes = [item for item in range(attributes) if "=" in item]
#     inputAttributes = []
#     for item in range(attributes):
#         # this is to avoid attempting to input white space, like " "
#         if "=" in item:
#             inputAttributes.append(item)
#             # input last, first, inputAttributes
#     dataString = " ".join(inputAttributes)
#     currTable.put_item(
#         Item={
#             'LastName': lastName,
#             'FirstName': firstName,
#             'memberData': dataString
#         }
#     )





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



def DeleteDynamoTable():
    try:
        dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
        table = dynamodb.Table('0727916mtkuweduDataMemberscss436')
        table.delete()
        return("Delete table success.")
    except:
        return("No data to delete.")