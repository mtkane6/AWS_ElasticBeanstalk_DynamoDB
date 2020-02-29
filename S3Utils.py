import boto3
import DynamoUtils
import os
import datetime



def CreateS3Bucket():
    s3 = boto3.resource("s3")
    try:
        newBucket = "0727916mtkuwedudatamemberscss436"
        if s3.Bucket("0727916mtkuwedudatamemberscss436") not in s3.buckets.all():
            s3.create_bucket(Bucket=newBucket, CreateBucketConfiguration={'LocationConstraint':'ap-northeast-1'})
        return("0727916mtkuwedudatamemberscss436")
    except Exception as e:
        print(e)
    

# creates local file with cleaned data from website
def CreateFileForS3(linesOfData):
    try:
        if os.path.exists("input.txt"):
            os.remove("input.txt")
        with open("input.txt", "a") as localTextFile:
            for i in range(len(linesOfData)-1):
                if len(linesOfData[i]) > 1:
                    try:
                        cleanedDataString = DynamoUtils.BuildDataMemberAttributes(linesOfData[i])
                        localTextFile.write(cleanedDataString + "\n")
                    except Exception as e:
                        print(e)
        return "input.txt"
    except Exception as e:
        print(e)

def CopyFileToS3(localFileName, bucketName):
    s3 = boto3.resource("s3")
    try:
        for root, dirs, filename in os.walk('.'): # walks files, and directories
            # print("Current directory: %s" % root)
            for currentFile in filename:
                slash = "/" if os.name == 'posix' else "\\"
                currentPath = root+slash+currentFile
                if localFileName in currentPath:
                    if not str.startswith(currentFile,'.DS_'): # for MacOS, this skips ".DS_Store" files, which serve indexing purposes.
                        # currentFileName = root + slash + currentFile
                        # get last modified time of local file
                        try:
                            localFileLastModified = datetime.datetime.utcfromtimestamp(os.path.getmtime(currentPath)).strftime('%Y-%m-%d %H:%M:%S')
                            bucket = s3.Bucket(bucketName)
                        except Exception as e:
                            print("Error: ", e)

                        # get last upload time of file in bucket
                        try:
                            obj = bucket.Object(currentPath).last_modified
                            objectFileLastModified = obj.strftime('%Y-%m-%d %H:%M:%S')
                            # check to see if file modified since last upload
                            if localFileLastModified > objectFileLastModified:
                                try:
                                    print("Currentfile: ", currentFile)
                                    s3.Object(bucketName, currentFile).put(Body=open(currentFile,"rb"))
                                    uploadSuccess = True
                                except Exception as e:
                                    print("upload error: ", e)
                            else:
                                # local file not modified since last upload
                                uploadSuccess = True
                        except Exception as e:
                            s3.Object(bucketName, currentFile).put(Body=open(currentFile,"rb"))
                            # uploaded file to bucket
                            uploadSuccess = True
        return(True)
    except Exception as e:
        print("Error during upload to s3 function: ", e)


def DeleteLocalFile():
    os.remove("input.txt")
    return

def DeleteS3Bucket():
    s3 = boto3.resource('s3')
    bucketName = '0727916mtkuwedudatamemberscss436'
    # first delete the object in the bucket, then the bucket iteself
    s3.Object(bucketName, 'input.txt').delete()
    s3.Bucket('0727916mtkuwedudatamemberscss436').delete()
    return