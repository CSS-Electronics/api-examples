# CANedge API Examples - MDF4 and S3

This project includes basic Python examples of how the MDF4 and S3 APIs can be used to automate data processing with your CANedge CAN/LIN data loggers.

For details on getting started with the APIs, see the [CANedge Intro](https://www.csselectronics.com/screen/page/can-logger-resources). 

---
## Features
```
- basics: asammdf API basics (e.g. how to DBC convert MDF4 data, transform it to pandas and plot it)
- basics: S3 basics (e.g. how to download, upload or list data on your server)
- basics: MDF converter (how to use the simple MDF4 converter executables in scripts)
- S3 events: AWS Lambda with MDF4 converters (how to automate your MDF4 processing on AWS)
- S3 events: MinIO notifications with MDF4 converters (how to automate your MDF4 processing on MinIO)
- misc: Light CSV DBC converter (extracting signal data from the mdf2csv converter output)
- utils: Basic e-mail sender function
```

---

## Requirements
- The scripts are tested using Python 3.7  
- Most scripts are designed for Windows, but can easily be modified for Linux  

---

## Usage info
- The scripts in this project are designed to be minimal and help you get started  
- The scripts are not designed for production and will require adjustment for your use case  
- The scripts are not covered by our technical support  
- Some S3 scripts use hardcoded credentials to ease testing - for production see e.g. [this guide](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html)

---

## Script guides  

### S3 events 
In many cases it can be useful to process log files immediately when they're uploaded - e.g. for predictive maintenance purposes or workflow optimization. 

Below, we describe one (of many) ways this can be setup on AWS S3 and MinIO S3, respectively. The examples take can be used to automatically run an MDF4 converter on each uploaded log file, transferring the output into a new bucket.

#### AWS Lambda
AWS Lambda lets you run code without provisioning or managing servers - learn more [here](https://docs.aws.amazon.com/lambda/latest/dg/welcome.html).

To test this, you can try the `aws_lambda_mdf_convert.py` code:  
1. Create an [IAM execution role](https://docs.aws.amazon.com/lambda/latest/dg/with-s3-example.html#with-s3-create-execution-role) incl. permissions: `AWSLambdaBasicExecutionRole` + `AmazonS3FullAccess`  
1. Create a target bucket for the converted files  
1. In Services/Lambda add a new function incl. a name, Python 3.7 and your execution role
1. Add S3 as trigger with your source bucket and 'All object create events' as event type  
1. Set the suffix to match your log file extension, e.g. `.MF4`, `.MFC`, `.MFE`, ...  
1. Download the `aws_lambda_mdf_convert.py` to a folder and update the `target_bucket`  
1. Add your preferred Linux (`*.AppImage`) MDF4 converter and the `passwords.json` file  
1. Zip the folder and upload the content via the AWS Lambda dropdown under Code entry type  
1. Change the Handler field to `aws_lambda_mdf_convert.lambda_handler` and hit Save  
1. Under Basic settings, set the timeout to e.g. 2 minutes (test based on your file size)  
1. Test by uploading a log file from the Home tab in CANcloud (monitor the CloudWatch logs)


#### MinIO Client (Listen Bucket Notifications)
The MinIO Client provides a simple interface to listen to bucket events and react.

To test this, you can try the `minio_listen_mdf_converter.py` code:  
1. Update the code with relevant suffix, converter path and MinIO server details
1. Run the code with your MinIO server by e.g. adding it to your server startup `*.bat`

---

## Explore further 
Below we list other resources for API documentation and examples.

### MDF4
The most popular tools for processing the CANedge log files are asammdf and our MDF4 converters:  
- [asammdf](https://github.com/danielhrisca/asammdf) - releases & documentation  
- [mdf4-conveters](https://github.com/CSS-Electronics/mdf4-converters) - releases & documentation  

### S3 
You can manage your S3 server via the AWS SDK or the derived MinIO SDK. Both are available in multiple programming languages incl. Python, Ruby, Javascript, Java, Go and more.

- [AWS S3 REST API](https://docs.aws.amazon.com/AmazonS3/latest/API/Welcome.html) - documentation  
- [AWS SDKs](https://aws.amazon.com/tools/) - documentation for the AWS S3 SDKs  
- [MinIO SDKs](https://docs.min.io/docs/javascript-client-quickstart-guide.html) - documentation for the MinIO S3 SDKs  

In particular, MinIO provides a set of useful script examples:  

- [MinIO Python SDK Examples](https://github.com/minio/minio-py)  
- [MinIO Javascript SDK Examples](https://github.com/minio/minio-js)  
- [MinIO Ruby SDK Examples](https://github.com/minio/minio-ruby)  
- [MinIO Java SDK Examples](https://github.com/minio/minio-java)  
- [MinIO Go SDK Examples](https://github.com/minio/minio-go)  

---
## About the CANedge

For details on installation and how to get started, see the documentation:
- [CANedge Resources](https://www.csselectronics.com/screen/page/can-logger-resources)  
- [CANedge1 Product Page](https://www.csselectronics.com/screen/product/can-logger-sd-canedge1/language/en)  
- [CANedge2 Product Page](https://www.csselectronics.com/screen/product/can-lin-logger-wifi-canedge2/language/en)  

---
## Contribution & support 
Feature suggestions, pull requests or questions are welcome!

You can contact us at CSS Electronics below:  
- [www.csselectronics.com](https://www.csselectronics.com)  
- [Contact form](https://www.csselectronics.com/screen/page/can-bus-logger-contact)  
- contact[AT]csselectronics.com  
