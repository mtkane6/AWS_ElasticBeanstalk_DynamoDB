from flask import Flask, request, render_template
import urllib.request
import DynamoUtils
import S3Utils
import string
import boto3


# EB looks for an 'application' callable by default.
application = Flask(__name__)

def GetDynamoDbTableName():
    return '0727916mtkuweduDataMemberscss436'

def GetS3BucketName():
    return '0727916mtkuwedudatamemberscss436'

def GetLocalFileName():
    return 'input.txt'

# DATA_LOADED = False
    

@application.route('/')
def HomePage():
    return render_template('index.html')

@application.route('/query/', methods=['POST'])
def QueryName():
    lastName = request.form['last'].lower().title()
    firstName = request.form['first'].lower().title()
    if not lastName and not firstName:
        return render_template('index.html', queryMessage = ['Enter a first and/or last name'])

    dynamodb = boto3.client('dynamodb', region_name='us-west-2')
    try:
        response = dynamodb.describe_table(TableName = GetDynamoDbTableName())
    except Exception as e:
        print('Query error: ', e)
        return render_template('index.html', queryMessage =['Database not yet loaded'])
    
    message = DynamoUtils.QueryDynamodb(GetDynamoDbTableName(), lastName, firstName)
    return render_template('index.html', queryMessage = message)
    # else:
    #     return render_template('index.html', queryMessage = ['First, click "Load" to load data into database'])

@application.route('/load/', methods=['POST'])
def LoadData():
    target_url = "https://s3-us-west-2.amazonaws.com/css490/input.txt"
    
    headers = {}
    headers['User-Agent'] = 'Mozilla/5.0 (X11; Linux i686) AppleWebKit/537.17 (KHTML, like Gecko) Chrome/24.0.1312.27 Safari/537.17'

    # get data from txt file in s3 bucket
    try:
        request = urllib.request.Request(target_url, headers=headers)
        resp = urllib.request.urlopen(request).read().decode('UTF-8')
    except Exception as e:
        errorString = "Error: %s" %e
        return errorString

    # creation of dynamodb table
    currTableName = DynamoUtils.CreateTable(GetDynamoDbTableName())

    # attempt to create s3 bucket to store parsed data
    bucketCreationName = S3Utils.CreateS3Bucket(GetS3BucketName())

    # create list of lines of member data from txt file
    textLines = resp.split('\r\n')

    # split each line of data into lists of individual strings
    dataList = [[line.split(' ')[i] for i in range(len(line.split(' ')))] for line in textLines]

    # create local file with data from url endpoint
    localFileName = S3Utils.CreateFileForS3(dataList, GetLocalFileName())

    # copy local file created above to s3 bucket
    s3Boolean = S3Utils.CopyFileToS3(localFileName, bucketCreationName)

    # upload data from local file to dynamodb
    localFileToDynamo = DynamoUtils.InputLocalFileDataToDynamoDB(localFileName, currTableName)

    # DATA_LOADED = True
    message = "Loaded data to local file, then s3 bucket, then dynamodb."
    return render_template('index.html', loadMessage=message)


@application.route('/delete/', methods=['POST'])
def DeleteData():
    noDataMessage = "No data to delete yet"
    # if not DATA_LOADED:
    #     return render_template('index.html', deleteMessage=noDataMessage)
    try:
        DynamoUtils.DeleteDynamoTable(GetDynamoDbTableName())
    except Exception as e:
        print("Error deleting dynamodb from appl.py: ", e)
        return render_template('index.html', deleteMessage=noDataMessage)
    try:
        S3Utils.DeleteLocalFile(GetLocalFileName())
    except Exception as e:
        print("Error deleting local file from app.py: ", e)
        return render_template('index.html', deleteMessage=noDataMessage)
    try:
        S3Utils.DeleteS3Bucket(GetS3BucketName())
    except Exception as e:
        print("Error deleting s3 bucket from app.py: ", e)
        return render_template('index.html', deleteMessage=noDataMessage)
    message = "Deleted local file, s3 bucket, and dynamodb."
    return render_template('index.html', deleteMessage=message)


# run the app.
if __name__ == "__main__":
    # Setting debug to True enables debug output. This line should be
    # removed before deploying a production app.
    application.debug = True
    application.run()

