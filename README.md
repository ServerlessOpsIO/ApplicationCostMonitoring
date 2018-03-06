# AWS Application Dollar Monitoring
[![serverless](http://public.serverless.com/badges/v3.svg)](http://www.serverless.com)
<!---[![Build Status](https://travis-ci.org/ServerlessOpsIO/aws-adm.svg?branch=master)](https://travis-ci.org/ServerlessOpsIO/aws-adm)-->
[![License](https://img.shields.io/badge/License-BSD%202--Clause-orange.svg)](https://opensource.org/licenses/BSD-2-Clause)

Application Dollar Monitoring provides granular AWS spend tracking.

Using the nightly generated AWS billing report, This system will parse the report and feed the line items to a place where you can perform analysis.  Currently supported is S3 which allows you to use Athena to query your billing items.

![System Architecture](/AWS%20ADM%20Diagram.png?raw=true "System Architecture")

## Deployment
### Application Deployment
Clone of this repository by using [Serverless Framework](https://serverless.com/).

```
$ npm install -g serverless
$ npm install
$ serverless deploy -v
```
### Billing Report Setup

Setup billing report.

Once a report is run and Glue crawler has run, the `line_items` table will be available in the application_dollar_monitoring database.

## Usage
Use [AWS Athena](https://aws.amazon.com/athena/) to query the data.
