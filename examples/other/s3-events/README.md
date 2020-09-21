# S3 events
Often it can be useful to process log files immediately when they're uploaded - e.g. for predictive maintenance or workflow.

Below, we describe one (of many) ways this can be setup on AWS S3 and MinIO S3, respectively. The examples take can be used to automatically run an MDF4 converter on each uploaded log file, transferring the output into a new bucket.

## AWS Lambda
AWS Lambda lets you run code without provisioning or managing servers - learn more [here](https://docs.aws.amazon.com/lambda/latest/dg/welcome.html).

To test this, you can try the `aws_lambda_mdf_convert.py` code:  
1. Create an [IAM execution role](https://docs.aws.amazon.com/lambda/latest/dg/with-s3-example.html#with-s3-create-execution-role) incl. permissions: `AWSLambdaBasicExecutionRole` + `AmazonS3FullAccess`  
1. Create a target bucket for the converted files  
1. In Services/Lambda add a new function incl. a name, Python 3.7 and your execution role
1. Add S3 as trigger with your source bucket and 'All object create events' as event type  
1. Set the suffix to match your log file extension, e.g. `.MF4`, `.MFC`, `.MFE`, ...  
1. Download the `aws_lambda_mdf_convert.py` to a folder and update the `target_bucket`  
1. Add a Linux MDF4 converter (update the `converter_name`) and the `passwords.json` file  
1. Zip the folder and upload the content via the AWS Lambda dropdown under Code entry type  
1. Change the Handler field to `aws_lambda_mdf_convert.lambda_handler` and hit Save  
1. Under Basic settings, set the timeout to e.g. 2 minutes (test based on your file size)  
1. Test by uploading a log file from the Home tab in CANcloud (monitor the CloudWatch logs)

Note: If your deployment package requires additional dependencies, you need to include these in the zip. To do this, you can use `pip install [module] --target .` in the folder.

## MinIO Client (Listen Bucket Notifications)
The MinIO Client provides a simple interface to listen to bucket events and react.

To test this, you can try the `minio_listen_mdf_convert.py` code:  
1. Update the code with relevant suffix, converter path and MinIO server details
1. Run the code with your MinIO server by e.g. adding it to your server startup `*.bat`
