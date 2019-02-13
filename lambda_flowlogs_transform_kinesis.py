"""
Adam Shechter
at1


"""


import os
import boto3
import gzip
import json
import datetime
import base64
from io import BytesIO

firehose_client = boto3.client('firehose')


# function to send record to Kinesis Firehose
def SendToFireHose(streamName, records):
    response = firehose_client.put_record_batch(
        DeliveryStreamName = streamName,
        Records=records
    )
    print(response)
    #log the number of data points written to Kinesis
    print("Wrote the following records to Firehose: " + str(len(records)))


def lambda_handler(event, context):
    # print(event['awslogs'])
    # capture the CloudWatch log data
    outEvent = str(event['awslogs']['data'])
    # decode and unzip the log data
    outEvent2 = base64.b64decode(outEvent)
    outEvent3 = gzip.GzipFile(fileobj=BytesIO(outEvent2)).read()
    print(outEvent3)
    # convert the log data from JSON into a dictionary
    cleanEvent = json.loads(outEvent3)
    print(cleanEvent)
    # initiate a list
    s = []
    # set the name of the Kinesis Firehose Stream
    firehoseName = os.environ['firehose_stream']

    """
    'logEvents': [{'id': '34556518727316219200749233816408550482522047377421172736', 'timestamp': 1549567892000, 'message': '2 671900666536 eni-3001219a 88.214.26.44 172.31.10.128 18606 4145 6 3 180 1549567892 1549567939 ACCEPT OK', 'extractedFields': {'srcaddr': '88.214.26.44', 'dstport': '4145', 'start': '1549567892', 'dstaddr': '172.31.10.128', 'version': '2', 'packets': '3', 'protocol': '6', 'account_id': '671900666536', 'interface_id': 'eni-3001219a', 'log_status': 'OK', 'bytes': '180', 'srcport': '18606', 'action': 'ACCEPT', 'end': '1549567939'}}
    """

    for line in cleanEvent['logEvents']:
        record_json = json.dumps(line['extractedFields'])
        current_record = {'Data': record_json.encode()}
        s.append(current_record)

      # limit of 500 records per batch. Break it up if you have to.
        if len(s) > 499:
            # send the response to Firehose in bulk
            SendToFireHose(firehoseName, s)

            # Empty the list
            s = []

    # when done, send the response to Firehose in bulk
    if len(s) > 0:
        SendToFireHose(firehoseName, s)
    return
