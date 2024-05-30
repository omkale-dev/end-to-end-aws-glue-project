# Deploying base infra

  

> these resources are execute once, so these are not part of CICD
- You can deploy it using cfn or you can take help of the below config and do it manually
- Make sure you make the changes to resource names
### s3_buckets
change the bucket names
deploy the stack

```sh

aws cloudformation deploy --template-file ./base_infra/s3_buckets.yaml --stack-name PfSssInfra --capabilities CAPABILITY_IAM

```

### glue_database

```sh

aws cloudformation deploy --template-file ./base_infra/glue_databases.yaml --stack-name PfGlueDb --capabilities CAPABILITY_IAM

```
### dynamodb_tables
change `<your ac id>` to your account
change `<your lambda role>` to the lambda role used for `error_handler_lambda` function
change  `<your admin user>` to the admin user in your account
change  `<your local user>` to your local user, used for local dev
deploy the stack

```sh

aws cloudformation deploy --template-file ./base_infra/dynamodb_tables.yaml --stack-name PfDynamodbInfra --capabilities CAPABILITY_IAM

```


### eventbridge_rule
change `<your cloudwatch log group arn>` to your cloudwatch group
change `<your event handler lambda arn>` to your event handler lambda
deploy the stack

```sh

aws cloudformation deploy --template-file ./base_infra/eventbridge_rule.yaml --stack-name PfEVBR --capabilities CAPABILITY_IAM

```

### sns_topic
change `<your email>` to your mailid
deploy the stack

```sh

aws cloudformation deploy --template-file ./base_infra/sns_topic.yaml --stack-name PfSnsTopic --capabilities CAPABILITY_IAM

```