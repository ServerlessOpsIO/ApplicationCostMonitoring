# AWS Application Cost Monitoring
[![serverless](http://public.serverless.com/badges/v3.svg)](http://www.serverless.com)
[![Build Status](https://travis-ci.org/ServerlessOpsIO/ApplicationCostMonitoring.svg?branch=master)](https://travis-ci.org/ServerlessOpsIO/ApplicationCostMonitoring)
[![License](https://img.shields.io/badge/License-BSD%202--Clause-orange.svg)](https://opensource.org/licenses/BSD-2-Clause)

Application Cost Monitoring provides granular AWS spend tracking.

Using the AWS Cost and Usage report this system will parse the report and feed the line items to a place where you can perform analysis.  This service only handles the billing report ingestion, parsing, and publishing to SNS.  Additional publisher services need to be deployed. eg.

* [ACM-S3-Publisher](https://github.com/ServerlessOpsIO/ACM-S3-Publisher)
* [ACM-DynamoDB-Publisher](https://github.com/ServerlessOpsIO/ACM-DynamoDB-Publisher)

![System Architecture](/diagram.png?raw=true "System Architecture")

## Deployment
Read through the entire documentation first.  There is information in setting up the billing report that may influence your deployment.

You will perform the folowing actions:
* Deploy the application
* Create the billing report
* Setup bucket policy (AWS Application Repository only)

### Application Deployment
This service supports both [Serverless Framework](https://serverless.com/) and [AWS Serverless Application Repository](https://aws.amazon.com/serverless/serverlessrepo/).

#### Serverless Framework
Clone this repository and deploy using Serverless Framework.

```
$ npm install -g serverless
$ npm install
$ serverless deploy -v
```

#### AWS Serverless Application Repository
This application is available in the AWS Serverless Application Repository.  Follow the directions there if you wish to deploy from AppRepo.

* https://serverlessrepo.aws.amazon.com/#/applications/arn:aws:serverlessrepo:us-east-1:641494176294:applications~ApplicationCostMonitoring

### Outputs
* _aws-adm-${stage}-BillingReportS3BucketName_: Name of S3 Bucket where billing reports will be delivered
* _aws-adm-${stage}-BillingRecordsSnsTopicArn_: ARN of SNS topic that data publishers are to connect to.
* _aws-adm-${stage}-AdmStateS3BucketName_: Name of the S3 bucket where state is stored during and between runs.

### Billing Report Setup
Setup the [AWS Billing cost and usage report](https://docs.aws.amazon.com/awsaccountbilling/latest/aboutv2/billing-reports-costusage.html).  This will deliver billing reports to the configured S3 bucket up to three times a day.  This service will create the S3 bucket for you when it is deployed.  Get the _aws-adm-${stage}-BillingReportS3BucketName_ stack export after deploying and 

Follow the AWS instructions for [turning on the Cost and Usage Report](https://docs.aws.amazon.com/awsaccountbilling/latest/aboutv2/billing-reports-gettingstarted-turnonreports.html).  Select hourly or daily report data depending on the granularity of data you need (and can afford considering the potential size).

Additional cost insight can be found by using cost allocation tags.  [Enable cost allocation tags](https://docs.aws.amazon.com/awsaccountbilling/latest/aboutv2/activate-built-in-tags.html) in the AWS console if desired and activate any appropriate [user defined tags](https://docs.aws.amazon.com/awsaccountbilling/latest/aboutv2/custom-tags.html).

To see what data is in the report, refer to the AWS documentation for [cost and Usage Report Details](https://docs.aws.amazon.com/awsaccountbilling/latest/aboutv2/billing-reports-costusage-details.html).  You decide the tags to track before you deploy Application Cost Monitoring.  Changing the tags tracked in billing reports will cause some line item's to change their ID.  Depending on how you are performing your analysis this may not be an issue.  If you are using AWS Athena to query the data then this will result in a schema change that will break querying.  You will also have to deal with duplicate line item data in the dataset unless you purge all previous data.  See the `SCHEMA_CHANGE_HANDLING` variable for more information.

### Configuration
*SCHEMA_CHANGE_HANDLING*: Set the desired behavior for how to handle a change in the billing report schema being detected.  If using CloudFormation or AWS SAM, set this parameter to one of the values below.  If using Serverless Framework, set value as an environmental variable.  Choose the correct option for you after reading below.  The default value is `CONTINUE`.

Changing tags on billing reports will alter the report schema which can:
- break downstream analysis systems dependent on the schema.
- result in item duplication as AWS will generate a new line item ID and this system has to continually reprocess 1st of month on every run.
- make tracking an item across the month difficult due to change in ID.

Options:
- ERROR: Error out line item writer. Must remove schema state file or remove tags to continue processing.
- CONTINUE: Just continue processing.
- RECONCILE: Reprocess the entire report.

## Usage
This service only handles billing report ingestion and parsing.  It requires an additional publisher service to be useful.  Available ones are:

* [ACM-S3-Publisher](https://github.com/ServerlessOpsIO/ACM-S3-Publisher)
* [ACM-DynamoDB-Publisher](https://github.com/ServerlessOpsIO/ACM-DynamoDB-Publisher)

