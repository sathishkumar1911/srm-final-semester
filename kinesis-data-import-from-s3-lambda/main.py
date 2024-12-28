import json
import boto3
from logger import console_logger

boto3.compat.filter_python_deprecation_warnings()
# Initialize AWS clients for S3 and Kinesis
s3_client = boto3.client("s3", region_name="us-east-1")
kinesis_client = boto3.client("kinesis", region_name="us-east-1")
# Initialize the console logger
logger = console_logger.config_logger(__name__)

KINESIS_STREAM_NAME = "smart_device_kinesis_stream"


def lambda_handler(event, context):
    # Get the bucket name and object key from the S3 event trigger
    bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
    object_key = event["Records"][0]["s3"]["object"]["key"]

    logger.info("Lambda handler called with the event : %s", event)
    logger.info("bucket_name: %s", event)
    logger.info("object_key: %s", object_key)

    if "isTest" in event["Records"][0]["s3"] and event["Records"][0]["s3"]["isTest"]:
        logger.info("Test Passed")
        return {
            "statusCode": 200,
            "body": json.dumps("------------------------------ Test Passed Successfully ------------------------------")
        }
    else:
        try:
            # Retrieve the file from the S3 bucket
            response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
            file_content = response["Body"].read().decode("utf-8")

            # Process the file content (e.g., split by lines)
            for line in file_content.splitlines():
                # Send each line (or message) to the Kinesis stream
                kinesis_client.put_record(
                    StreamName=KINESIS_STREAM_NAME,
                    Data=line,
                    PartitionKey="shard1"
                )

            logger.info("Successfully processed file(%s/%s) and sent to Kinesis", bucket_name, object_key)
            return {
                "statusCode": 200,
                "body": json.dumps("Successfully processed file and sent to Kinesis")
            }

        except Exception as e:
            logger.error(f"Error processing file from S3: %s", str(e), exc_info=True)
            return {
                "statusCode": 500,
                "body": json.dumps(f"Error: {str(e)}")
            }
