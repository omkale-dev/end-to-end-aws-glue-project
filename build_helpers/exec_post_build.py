import os
import sys

def snake_to_camel(snake_case_str):
    words = snake_case_str.split('_')
    # Capitalize the first letter of each word (except the first one)
    camel_case_words = [words[0].lower()] + [word.capitalize() for word in words[1:]]
    # Join the words to form the camel case string
    camel_case_str = ''.join(camel_case_words)
    return camel_case_str


def find_job_template_yaml_in_glue_jobs():
    # Get the current directory of the Python script
    # current_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Get the parent directory
    # parent_dir = os.path.dirname(current_dir)
    
    # Check if "glue_jobs" directory exists at the parent level
    # glue_jobs_dir = os.path.join(parent_dir, "glue_jobs")
    glue_jobs_dir = "./glue_jobs"
    if os.path.exists(glue_jobs_dir) and os.path.isdir(glue_jobs_dir):
        job_template_yaml_dict = {}
        # Iterate through the contents of "glue_jobs" directory
        for root, dirs, files in os.walk(glue_jobs_dir):
            for file in files:
                if file == "job_template.yaml":
                    # Construct path relative to "glue_jobs" directory
                    relative_path = os.path.relpath(root, glue_jobs_dir)
                    folder_name = relative_path.split(os.path.sep)[0]
                    yaml_path = os.path.join("./glue_jobs", relative_path, file)
                    if folder_name not in job_template_yaml_dict:
                        job_template_yaml_dict[folder_name] = [yaml_path]
                    else:
                        job_template_yaml_dict[folder_name].append(yaml_path)
        return job_template_yaml_dict
    else:
        print("No 'glue_jobs' directory found at the parent level.")
        return {}

def process_glue_templates():
    print("Job Template YAML Files Paths:")
    job_template_yaml_dict = find_job_template_yaml_in_glue_jobs()
    for folder_name , yaml_paths in job_template_yaml_dict.items():
        print("Folder:", folder_name)
        for path in yaml_paths:
            template_file = path
            print("path:",path)
            stack_name= snake_to_camel(folder_name)
            aws_command = f"aws cloudformation deploy --template-file {template_file} --stack-name GlueJobStack{stack_name} --capabilities CAPABILITY_IAM --region us-east-1 --force-upload"
            print(f"Deploying stack:  GlueJob{stack_name}")
            os.system(aws_command)
            print("Deployed the stack!!")

def build_ingest_lambda():
    # package.yaml was created in previous step
    aws_command_1 = f"aws cloudformation deploy --template-file ./lambda_functions/pf_ingest/packaged.yaml --stack-name PfIngest --capabilities CAPABILITY_IAM --force-upload"
    os.system(aws_command_1)


arg1 = sys.argv[1]
print("The argument that was passed by buildspec is:",arg1)
if arg1 == "dev":
    build_ingest_lambda()
    process_glue_templates()
else:
    # you can implement the logic for other env, like changing bucket names, and other resources params etc... 
    pass   
print("Post-Build stage was completed")
