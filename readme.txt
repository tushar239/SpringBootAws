You can upload this application using AWS Beanstalk as shown in below youtube video

https://www.youtube.com/watch?v=Jq7qA4VLV04

You will need to assign proper role to EC2 via Beanstalk


To connect to DynamoDB via Beanstalk,
this role should be created through IAM service that has both Beanstalk and DynamoDB access.