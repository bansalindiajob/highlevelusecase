# EMR Local Container

## Getting Started

Thanks for installing your local EMR environment. To get started, there are a few steps.

- Login to ECR with the following command:


        aws ecr get-login-password --region us-west-1 \
        | docker login \
            --username AWS \
            --password-stdin \
            608033475327.dkr.ecr.us-west-1.amazonaws.com

- Use the `Remote-Containers: Reopen in Container` command to build your new environment.

## Usage tips

- You can start a new shell with the `pyspark` command in a terminal.
    - If you've configured your AWS credentials in `.env`, you should have access to everything you need.
- A sample PySpark script has been created for you in the `emr_tools_demo.py` file.



