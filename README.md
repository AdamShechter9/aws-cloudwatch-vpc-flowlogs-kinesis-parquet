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
Please calculate the volume of log data that will be generated, and be sure to create a Kinesis Data Firehose stream that can handle this volume.
If the stream cannot handle the volume, the log stream will be throttled.

Copyright 2019 At1 LLC
Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the Software), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions: The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED 'AS IS', WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
