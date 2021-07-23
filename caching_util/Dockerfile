# use amazonlinux:2 to best simulate the environment of AWS lambda
FROM amazonlinux:2
WORKDIR /lib/dependency/python
# disdat needs to work with python>=3.8
RUN amazon-linux-extras install python3.8
RUN yum install zip -y

# install disdat and remove boto3, botocore as they are provided by AWS lambda
# this is important as the zip file cannot be  bigger than 50MB
# also remove pycache and other redundant information to minimize zip file size 
CMD ["sh", "-c",
 "python3.8 -m pip install disdat -t . &&  \
 python3.8 -m pip uninstall boto3 botocore -y && \
 find . -name \"*.dist-info\"  -exec rm  -rf {} \; && \
 find . -name \"__pycache__\"  -exec rm  -rf {} \;"]
