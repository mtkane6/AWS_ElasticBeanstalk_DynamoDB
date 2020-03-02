import boto3
import DynamoUtils
import os
import datetime

def CreateS3Bucket(bucketName):

    # create bucket
    s3 = boto3.resource("s3")
    try:
        if s3.Bucket(bucketName) not in s3.buckets.all():
            s3.create_bucket(Bucket=bucketName, CreateBucketConfiguration={'LocationConstraint':'us-west-2'})
    except Exception as e:
        print(e)


def CleanDataString(linesOfData):
    returnTextList = []
    for i in range(len(linesOfData)-1):
        if len(linesOfData[i]) > 1:
            try:
                cleanedDataString = DynamoUtils.BuildDataMemberAttributes(linesOfData[i])
                returnTextList.append(cleanedDataString)
            except Exception as e:
                print(e)
    return "\n".join(returnTextList)


def WriteDataToS3(cleanedDataList, bucketName):
    try:
        DeleteS3BucketObject()
    except Exception as e:
        print("Ignoring attempt to delete bucket object that might not exist")

    s3 = boto3.resource('s3', region_name='us-west-2')
    response = s3.Object(bucketName, 'Input.txt').put(Body=cleanedDataList)


def DeleteS3BucketObject(bucketName, fileName):
    s3 = boto3.resource('s3')
    # first delete the object in the bucket, then the bucket iteself
    s3.Object(bucketName, fileName).delete()
    # s3.Bucket(bucketName).delete()
    return