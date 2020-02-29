from flask import Flask, request, render_template
import urllib.request
import DynamoUtils
import S3Utils
# import boto3


# EB looks for an 'application' callable by default.
application = Flask(__name__)

@application.route('/')
def HomePage():
    return render_template('index.html')

@application.route('/query/', methods=['POST'])
def QueryName():
    lastName = request.form['last']
    firstName = request.form['first']
    message = firstName + ", " + lastName
    return render_template('index.html', queryMessage = message)

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
    currTableName = DynamoUtils.CreateTable()
    # if not currTableName:
    #     return("Error creating DynamoDB table")

    # attempt to create s3 bucket to store parsed data
    bucketCreationName = S3Utils.CreateS3Bucket()
    # if not bucketCreationName:
    #     return("Error creating s3 bucket")

    # create list of lines of member data from txt file
    textLines = resp.split('\r\n')

    # split each line of data into lists of individual strings
    dataList = [[line.split(' ')[i] for i in range(len(line.split(' ')))] for line in textLines]

    localFileName = S3Utils.CreateFileForS3(dataList)
    # if not localFileName:
    #     return("Error creating local file")

    s3Boolean = S3Utils.CopyFileToS3(localFileName, bucketCreationName)
    # if not s3Boolean:
    #     return("Error during upload of data to s3 storage")

    localFileToDynamo = DynamoUtils.InputLocalFileDataToDynamoDB(localFileName, currTableName)


    message = "Loaded data to local file, then s3 bucket, then dynamodb."
    return render_template('index.html', loadMessage=message)


@application.route('/delete/', methods=['POST'])
def DeleteData():
    message = "Deleted local file, s3 bucket, and dynamodb."
    DynamoUtils.DeleteDynamoTable()
    S3Utils.DeleteLocalFile()
    return render_template('index.html', deleteMessage=message)


# run the app.
if __name__ == "__main__":
    # Setting debug to True enables debug output. This line should be
    # removed before deploying a production app.
    application.debug = True
    application.run()