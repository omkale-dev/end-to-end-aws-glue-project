import os
import sys

# ensure all the buckets, dynamodb tables, sns etc are provisioned first.

arg1 = sys.argv[1]
print("The argument that was passed by buildspec is:",arg1)
def sync_cc_s3():
    aws_command = f"aws s3 sync ./glue_jobs s3://pf-code-assets/"
    os.system(aws_command)

if arg1=="dev":
    sync_cc_s3()
else:
    # you can implement the logic for other env, like changing bucket names, and other resources params etc...
    pass    
print("Pre-Build stage was completed")
