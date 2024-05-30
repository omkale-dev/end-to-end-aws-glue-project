import os
import sys


arg1 = sys.argv[1]
print("The argument that was passed by buildspec is:", arg1)


def build_ingest_lambda():
    aws_command_1 = f"sam build --template ./lambda_functions/pf_ingest/template.yaml"
    aws_command_2 = f"sam package --s3-bucket pf-code-pipeline-bucket --force-upload --output-template-file ./lambda_functions/pf_ingest/packaged.yaml --template-file .aws-sam/build/template.yaml"
    os.system(aws_command_1)
    os.system(aws_command_2)


if arg1 == "dev":
    build_ingest_lambda()
else:
    # you can implement the logic for other env, like changing bucket names, and other resources params etc...
    pass
print("Build stage was completed")
