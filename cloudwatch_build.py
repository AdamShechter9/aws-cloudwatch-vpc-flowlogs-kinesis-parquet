#!/bin/python3
"""
At1Security - Adam Shechter

Script to generate resources and start CloudWatch VPC FlowLogs logging on AWS
Automatically applies logging to ALL VPC Groups

Steps
1.  Collect all VPCs in region
2.  Create IAM role for cloudwatch flow logs
3.  Create CloudWatch Flowlogs log group destination for VPCs
4.  Send VPC FlowLogs to cloudwatch
5.  Create S3 destination Bucket
6.  Create IAM role for Transformation Lambda
7.  Create Lambda for flowlogs tranformation to JSON
8.  Create IAM role for Kinesis Data Firehose
9.  Create Glue Database and Table Schema for FlowLogs
10. Create a Kinesis Data Firehose stream
11. Create a subscription filter from FlowLogs to Lambda
12. Create IAM Service role for crawler w. policies (AWSGlueServiceRole)
13. Create aws glue crawler for flowlogs parquet
14. Run glue crawler to create new table in aws glue

Based on:
https://aws.amazon.com/blogs/big-data/analyze-and-visualize-your-vpc-network-traffic-using-amazon-kinesis-and-amazon-athena/


NOTES:
This solution subscribes all VPC's to single log.
Calculate the volume of log data that will be generated.
Be sure to create a Kinesis Data Firehose stream that can handle this volume.
If the stream cannot handle the volume, the log stream will be throttled.

Resources created:
  IAM Role(s)
  VPC Flow Log
  CW Logs Log Group
  S3 Bucket
  Kinesis Firehose
     Pre-process Lambda
  KDA
     Pre-process Lambda
     Destination Lambda
  Glue Data Catalog Database
     Table
  CW Logs Subscription Filter
  CW Dashboard
"""

import json
import boto3
import sys
import os
import time
import string
import random
import logging


# Initialize logger object
def initialize_logger(output_dir):
    logger_obj = logging.getLogger()
    logger_obj.setLevel(logging.INFO)
    # create console handler and set level to info
    handler1 = logging.StreamHandler()
    handler1.setLevel(logging.INFO)
    formatter = logging.Formatter("%(levelname)s - %(message)s")
    handler1.setFormatter(formatter)
    logger_obj.addHandler(handler1)

    # create error file handler and set level to error
    handler2 = logging.FileHandler(os.path.join(output_dir, "error.log"), "w", encoding=None, delay="true")
    handler2.setLevel(logging.ERROR)
    formatter = logging.Formatter("%(levelname)s - %(message)s")
    handler2.setFormatter(formatter)
    logger_obj.addHandler(handler2)
    return logger_obj


# Returns list of all VPCs
def get_VPC_list() -> list:
    try:
        response = ec2_client.describe_vpcs()
    except Exception as e:
        print(e)
        logger.error(e)
        sys.exit(1)
    # logger.info(response)
    vpc_list = [x['VpcId'] for x in response['Vpcs']]
    return vpc_list


# Create IAM role and policy for VPC FlowLogs to push logs to Cloudwatch
# Return ARN for IAM role
def create_role_cloudwatch() -> str:
    try:
        with open("vpc_flowlogs_assume_role.json", "r") as f:
            assume_role_policy = f.read()
    except Exception as e:
        print(e)
        logger.error(e)
        sys.exit(1)
    try:
        response = iam_client.create_role(
            RoleName=cloudwatch_vpc_iam_role_name,
            AssumeRolePolicyDocument=assume_role_policy,
            Description='Automated Role for EC2 VPC log delivery to Cloudwatch',
        )
    except Exception as e:
        print(e)
        logger.error(e)
        sys.exit(1)
    logger.info(response['Role'])
    role_arn = response['Role']['Arn']
    try:
        with open("flowlogs_policy.json", "r") as f:
            role_policy = f.read()
    except Exception as e:
        print(e)
        logger.error(e)
        sys.exit(1)
    time.sleep(10)
    logger.info("creating policy and applying to role")
    try:
        response = iam_client.put_role_policy(
            RoleName=cloudwatch_vpc_iam_role_name,
            PolicyName='create_put_logs_cloudwatch_policy',
            PolicyDocument=role_policy
        )
    except Exception as e:
        print(e)
        logger.error(e)
        sys.exit(1)
    # logger.info(response)
    return role_arn


# Create a new log cloudwatch log group for FlowLogs
# Get CloudWatch Log Group ARN and return
def logs_create_log_group() -> str:
    try:
        response = logs_client.create_log_group(
            logGroupName=log_group_name,
        )
    except Exception as e:
        print(e)
        logger.error(e)
        sys.exit(1)
    try:
        response = logs_client.put_retention_policy(
            logGroupName=log_group_name,
            retentionInDays=731
        )
    except Exception as e:
        print(e)
        logger.error(e)
    try:
        response = logs_client.describe_log_groups(
            logGroupNamePrefix=log_group_name
        )
    except Exception as e:
        print(e)
        logger.error(e)
        sys.exit(1)
    # logger.info(response)
    logger.info(response['logGroups'][0]['arn'])
    return response['logGroups'][0]['arn']


def create_flow_log():
    # logger.info(delivery_role, log_group_name)
    try:
        response = ec2_client.create_flow_logs(
            DeliverLogsPermissionArn=delivery_role_arn,
            LogGroupName=log_group_name,
            ResourceIds=vpc_list,
            ResourceType='VPC',
            TrafficType='ALL',
            # LogDestinationType='cloud-watch-logs',
            # LogDestination=log_group_arn        # ARN of cloudwatch log group
        )
    except Exception as e:
        print(e)
        logger.error(e)
        sys.exit(1)
    logger.info(response)
    return


# Create S3 bucket for CloudTrail logging.
def s3_create_bucket(bucket_name):
    try:
        if region_name != 'us-east-1':
            response = s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={
                    'LocationConstraint': region_name
                },
            )
        else:
            response = s3_client.create_bucket(
                Bucket=bucket_name,
            )
        logger.info(response)
    except Exception as e:
        print(e)
        logger.error(e)
    return


def create_role_lambda() -> str:
    try:
        with open("lambda_assume_role.json", "r") as f:
            assume_role_policy = f.read()
    except Exception as e:
        print(e)
        logger.error(e)
        sys.exit(1)
    try:
        response = iam_client.create_role(
            RoleName=lambda_flowlogs_kinesis_role_name,
            AssumeRolePolicyDocument=assume_role_policy,
            Description='Automated Role for Lambda function to process flowlogs to kinesis',
        )
    except Exception as e:
        print(e)
        logger.error(e)
        sys.exit(1)
    logger.info(response['Role'])
    role_arn = response['Role']['Arn']
    try:
        with open("lambda_transform_cw_kinesis_policy.json", "r") as f:
            role_policy = f.read()
    except Exception as e:
        print(e)
        logger.error(e)
        sys.exit(1)
    time.sleep(15)
    logger.info("creating policy and applying to role")
    try:
        response = iam_client.put_role_policy(
            RoleName=lambda_flowlogs_kinesis_role_name,
            PolicyName='lambda_flowlogs_kinesis_policy',
            PolicyDocument=role_policy
        )
    except Exception as e:
        print(e)
        logger.error(e)
        sys.exit(1)
    return role_arn


def create_flowlogs_kinesis_lambda_function() -> str:
    try:
        with open('lambda_flowlogs_kinesis_package.zip', 'rb') as f:
            lambda_package = f.read()
    except Exception as e:
        print(e)
        logger.error(e)
        sys.exit(1)

    try:
        response = lambda_client.create_function(
            FunctionName=lambda_flowlogs_kinesis_name,
            Runtime='python3.6',
            Role=lambda_role_arn,
            Handler='lambda_flowlogs_transform_kinesis.lambda_handler',
            Code={
                'ZipFile': lambda_package,
            },
            Description='Lambda function for parsing FlowLogs to JSON for kinesis',
            Timeout=30,
            MemorySize=128,
            Publish=True,
            Environment={
                'Variables': {
                    'firehose_stream': kinesis_stream_name
                }
            },
        )
        logger.info(response)
    except Exception as e:
        print(e)
        logger.error(e)
        sys.exit(1)
    return response['FunctionArn']


# Create IAM role and policy for VPC FlowLogs to push logs to Cloudwatch
# Return ARN for IAM role
def create_role_kinesis() -> str:
    try:
        with open("kinesis_firehose_s3_role.json", "r") as f:
            assume_role_policy = f.read().replace("{{accountId}}", account_id)
    except Exception as e:
        print(e)
        logger.error(e)
        sys.exit(1)
    try:
        response = iam_client.create_role(
            RoleName=kinesis_iam_role_name,
            AssumeRolePolicyDocument=assume_role_policy,
            Description='Automated Role for Kinesis Data access to S3',
        )
    except Exception as e:
        print(e)
        logger.error(e)
        sys.exit(1)
    logger.info(response['Role'])
    role_arn = response['Role']['Arn']
    try:
        with open("firehose_s3_policy.json", "r") as f:
            role_policy = f.read().replace("{{bucketName}}", bucket_name)
    except Exception as e:
        print(e)
        logger.error(e)
        sys.exit(1)
    time.sleep(10)
    logger.info("creating policy and applying to role")
    try:
        response = iam_client.put_role_policy(
            RoleName=kinesis_iam_role_name,
            PolicyName='fireshose_s3_access_policy',
            PolicyDocument=role_policy
        )
    except Exception as e:
        print(e)
        logger.error(e)
        sys.exit(1)
    # logger.info(response)
    return role_arn


def put_subscription_filter():
    logger.info("Adding cloudwatch invoke permissions for Lambda.")
    try:
        response = lambda_client.add_permission(
            FunctionName=lambda_arn,
            StatementId='ID-1',
            Action='lambda:InvokeFunction',
            Principal='logs.' + region_name + '.amazonaws.com',
            SourceArn=log_group_arn,
            SourceAccount=account_id,
        )
        logger.info(response)
    except Exception as e:
        print(e)
        logger.error(e)
        sys.exit(1)
    time.sleep(10)
    try:
        response = logs_client.put_subscription_filter(
            logGroupName=log_group_name,
            filterName=log_subscription_name,
            filterPattern='[version, account_id, interface_id, srcaddr != "-", dstaddr != "-", srcport != "-", dstport != "-", protocol, packets, bytes, start, end, action, log_status]',
            destinationArn=lambda_arn
        )
        logger.info(response)
    except Exception as e:
        print(e)
        logger.error(e)
        sys.exit(1)
    return


def create_glue_resources():
    logger.info("Creating Glue Database: {0}".format(database_name))
    try:
        response = glue_client.create_database(
            DatabaseInput={
                'Name': database_name,
                'Description': 'Database containing tables of VPC flow log records.',
            }
        )
        logger.info(response)
    except Exception as e:
        print(e)
        logger.error(e)
        sys.exit(1)
    time.sleep(10)
    logger.info("Creating Glue Table: {0}".format(table_name))
    try:
        response = glue_client.create_table(
            DatabaseName=database_name,
            TableInput={
                'Name': table_name,
                'Description': 'Table of VPC flow logs.',
                'StorageDescriptor': {
                    'Columns': [
                        {
                            "Name": "start",
                            "Type": "bigint"
                        },
                        {
                            "Name": "end",
                            "Type": "bigint"
                        },
                        {
                            "Name": "srcaddr",
                            "Type": "string"
                        },
                        {
                            "Name": "dstaddr",
                            "Type": "string"
                        },
                        {
                            "Name": "srcport",
                            "Type": "int"
                        },
                        {
                            "Name": "dstport",
                            "Type": "int"
                        },
                        {
                            "Name": "protocol",
                            "Type": "int"
                        },
                        {
                            "Name": "packets",
                            "Type": "int"
                        },
                        {
                            "Name": "bytes",
                            "Type": "int"
                        },
                        {
                            "Name": "logstatus",
                            "Type": "string"
                        },
                        {
                            "Name": "action",
                            "Type": "string"
                        },
                    ],
                    'Location': 's3://' + bucket_name,
                    'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                    'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                    'SerdeInfo': {
                        'SerializationLibrary': 'org.openx.data.jsonserde.JsonSerDe',
                    },
                },
            }
        )
        logger.info(response)
    except Exception as e:
        print(e)
        logger.error(e)
        sys.exit(1)
    time.sleep(10)
    return


def create_kinesis_delivery_stream():
    try:
        response = firehose_client.create_delivery_stream(
            DeliveryStreamName=kinesis_stream_name,
            ExtendedS3DestinationConfiguration={
                'RoleARN': kinesis_role_arn,
                'BucketARN': 'arn:aws:s3:::' + bucket_name,
                'BufferingHints': {
                    'SizeInMBs': 128,
                    'IntervalInSeconds': 300
                },
                'CompressionFormat': 'UNCOMPRESSED',
                'DataFormatConversionConfiguration': {
                    'SchemaConfiguration': {
                        'RoleARN': kinesis_role_arn,
                        'DatabaseName': database_name,
                        'TableName': table_name,
                        'Region': region_name,
                    },
                    'InputFormatConfiguration': {
                        'Deserializer': {
                            'OpenXJsonSerDe': {},
                        }
                    },
                    'OutputFormatConfiguration': {
                        'Serializer': {
                            'ParquetSerDe': {},
                        }
                    },
                    'Enabled': True
                }
            }
        )
    except Exception as e:
        print(e)
        logger.error(e)
        sys.exit(1)
    return


def create_role_crawler() -> str:
    try:
        with open("glue_crawler_role.json", "r") as f:
            assume_role_policy = f.read()
    except Exception as e:
        print(e)
        logger.error(e)
        sys.exit(1)
    try:
        response = iam_client.create_role(
            RoleName=glue_crawler_role_name,
            AssumeRolePolicyDocument=assume_role_policy,
            Description='Automated Role for Glue Crawler to crawl parquet flowlogs',
        )
    except Exception as e:
        print(e)
        logger.error(e)
        sys.exit(1)
    logger.info(response['Role'])
    role_arn = response['Role']['Arn']
    try:
        with open("glue_crawler_service_policy.json", "r") as f:
            role_policy = f.read().replace("{{bucketName}}", bucket_name)
    except Exception as e:
        print(e)
        logger.error(e)
        sys.exit(1)
    time.sleep(15)
    logger.info("creating policy and applying to role")
    try:
        response = iam_client.put_role_policy(
            RoleName=glue_crawler_role_name,
            PolicyName='glue_crawler_access_policy',
            PolicyDocument=role_policy
        )
    except Exception as e:
        print(e)
        logger.error(e)
        sys.exit(1)
    return role_arn


def create_glue_crawler():
    try:
        response = glue_client.create_crawler(
            Name=glue_crawler_name,
            Role=glue_crawler_role_arn,
            DatabaseName=database_name,
            Description='Automated crawler to crawl parquet vpc flowlogs.',
            Targets={
                'S3Targets': [
                    {
                        'Path': 's3://' + bucket_name,
                        'Exclusions': [
                            'string',
                        ]
                    },
                ]
            },
            Schedule='cron(15 12 * * ? *)',
            TablePrefix='vpc_flowlogs_parquet_',
        )
    except Exception as e:
        print(e)
        logger.error(e)
        sys.exit(1)
    return


def start_crawler():
    try:
        glue_client.start_crawler(
            Name=glue_crawler_name
        )
    except Exception as e:
        print(e)
        logger.error(e)
        sys.exit(1)
    return

# return a random 6 character string for application name
def randomstring():
    chars = string.ascii_lowercase + string.digits
    return ''.join(random.choice(chars) for x in range(6))


if __name__ == '__main__':
    args = sys.argv[1:]
    if not args:
        print("This program generates CloudWatch FlowLogs for an AWS account\nusage: [profile_name] [account_id] [region_name]")
        sys.exit(1)
    else:
        profile_name = args[0]
        account_id = args[1]
        region_name = args[2]
    logger = initialize_logger('./')
    logger.info("Starting VPC CloudWatch FlowLogs solution build")
    try:
        session = boto3.Session(profile_name=profile_name)
        # Any clients created from this session will use credentials
        ec2_client = session.client('ec2', region_name=region_name)
        s3_client = session.client('s3', region_name=region_name)
        sts_client = session.client('sts', region_name=region_name)
        logs_client = session.client('logs', region_name=region_name)
        iam_client = session.client('iam')
        glue_client = session.client('glue', region_name=region_name)
        firehose_client = session.client('firehose', region_name=region_name)
        lambda_client = session.client('lambda', region_name=region_name)
        athena_client = session.client('athena', region_name=region_name)
    except Exception as e:
        print(e)
        logger.error(e)
        raise Exception("Error with AWS credentials")
    # Generate Names
    stack_name = "-app-" + randomstring()
    cloudwatch_vpc_iam_role_name = "flowlogs-delivery-role" + stack_name
    log_group_name = "vpc-cloudwatch-flowlogs-logs" + stack_name
    log_subscription_name = "flowlogs-subscription-filter" + stack_name
    bucket_name = "cloudwatch-vpc-flowlogs-" + region_name + stack_name
    lambda_flowlogs_kinesis_role_name = "lambda-flowlogs-kinesis-role" + stack_name
    lambda_flowlogs_kinesis_name = "lambda-flowlogs-kinesis" + stack_name
    kinesis_iam_role_name = "kinesis-to-s3-role" + stack_name
    kinesis_stream_name = "vpc-flowlogs-kinesis-stream" + stack_name
    database_name = "vpc-flow-logs-db" + stack_name
    table_name = "vpc-flow-logs-table" + stack_name
    glue_crawler_role_name = "glue-crawler-flowlogs-kinesis-role" + stack_name
    glue_crawler_name = "glue-crawler-vpc-flowlogs" + stack_name
    # Start Process
    logger.info("1. Collecting all VPCs in region")
    vpc_list = get_VPC_list()
    logger.info(vpc_list)
    logger.info("2. Creating IAM role and policy for flowlog delivery")
    logger.info("IAM ROLE: {0}".format(cloudwatch_vpc_iam_role_name))
    delivery_role_arn = create_role_cloudwatch()
    time.sleep(15)
    logger.info("3. Creating CloudWatch Log Group")
    logger.info("LOG GROUP NAME: {0}".format(log_group_name))
    log_group_arn = logs_create_log_group()
    time.sleep(20)
    logger.info("4. Creating VPC Flow Logs to deliver logs to CloudWatch")
    create_flow_log()
    logger.info("5. Creating S3 destination bucket for kinesis")
    logger.info("S3 BUCKET NAME: {0}".format(bucket_name))
    s3_create_bucket(bucket_name)
    logger.info("6.  Create IAM role for JSON Format Lambda")
    lambda_role_arn = create_role_lambda()
    logger.info("7.  Create Lambda for flowlogs tranformation to JSON")
    lambda_arn = create_flowlogs_kinesis_lambda_function()
    logger.info("8. Create IAM role for Kinesis Data Firehose to access S3")
    logger.info("KINESIS IAM ROLE: {0}".format(kinesis_iam_role_name))
    kinesis_role_arn = create_role_kinesis()
    logger.info("9.  Create Glue Database and Table Schema")
    create_glue_resources()
    logger.info("10. Create a Kinesis Data Firehose delivery stream")
    create_kinesis_delivery_stream()
    logger.info("Wait until Stream becomes active")
    # Could instead check on the resource and see if it's active
    time.sleep(180)
    logger.info("11. Create a subscription filter from FlowLogs to Lambda")
    put_subscription_filter()
    logger.info("12. Create IAM Service role for crawler w. policies (AWSGlueServiceRole)")
    glue_crawler_role_arn = create_role_crawler()
    logger.info("13. Create aws glue crawler for flowlogs parquet")
    time.sleep(5)
    create_glue_crawler()
    logger.info("Waiting 10 mins for parquet data to show up in S3 to run crawler.")
    # Could instead check for objects in the s3 bucket.  if it's not empty, than we can run.
    time.sleep(600)
    logger.info("14. Run glue crawler to create new table in aws glue")
    start_crawler()
