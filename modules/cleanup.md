# Building Serverless Data Lakes on AWS

Author: Unni Pillai | Amazon Web Services | [Twitter](https://twitter.com/unni_k_pillai) | [Linkedin](http://bit.ly/upillai)

Updated by: Vikas Omer | Amazon Web Services | [Linkedin](https://www.linkedin.com/in/vikas-omer/)


# Clean Up

Failing to do this will result in incuring AWS usage charges.

Make sure you bring down / delete all resources created as part of this lab

## Resources to delete
* Kinesis Firehose Delivery Stream
	* GoTo: https://console.aws.amazon.com/firehose/home?region=us-east-1#/
	* Delete Firehose:  **sg-summit-demo-stream**
* Lambda
	* GoTo: https://console.aws.amazon.com/lambda/home?region=us-east-1
	* Navigate to list of functions and select **top5Songs**.
	* Under **Actions** drop down menu, select **Delete**.
* Glue Database
	* GoTo: https://console.aws.amazon.com/glue/home?region=us-east-1#catalog:tab=databases
	* Delete Database: **summitdb**
* Glue Crawler
	* GoTo: https://console.aws.amazon.com/glue/home?region=us-east-1#catalog:tab=crawlers
	* Delete Crawler: **summitcrawler**
* Glue Dev Endpoint
	* GoTo: https://console.aws.amazon.com/glue/home?region=us-east-1#etl:tab=devEndpoints
	* Delete endpoint: **devendpoint1**
* Sagemaker Notebook
	* You may wish you download the notebook file locally on your laptop before deleting the notebook)
	* GoTo: https://console.aws.amazon.com/glue/home?region=us-east-1#etl:tab=notebooks
	* Delete Notebook: **aws-glue-notebook1**
* Delete IAM Role
	* GoTo: https://console.aws.amazon.com/iam/home?region=us-east-1#/roles
	* Search for AWSGlueServiceRoleDefault
	* Delete Role: **AWSGlueServiceRoleDefault**
	* Search for top5Songs in search box 
	* Select and delete this role. [top5Songs-role-<id>]
* Delete S3 bucket
	* GoTo: https://s3.console.aws.amazon.com/s3/home?region=us-east-1
	* Delete Bucket: **yourname-datalake-demo-bucket**
* Delete Cognito Setup :
	* Goto: https://us-west-2.console.aws.amazon.com/cloudformation/home?region=us-west-2#/stacks/
	* Click: **Kinesis-Data-Generator-Cognito-User**
	* Click: **Actions** > **DeleteStack**
	* On confirmation screen: Click: **Delete**
* Close QuickSight account
	* GoTo: https://us-east-1.quicksight.aws.amazon.com/sn/admin#permissions
	* Click: **Unsubscribe**
* Cognito Userpool
	* GoTo: https://us-west-2.console.aws.amazon.com/cognito/users/?region=us-west-2#/


> Back to [main page](../readme.md)