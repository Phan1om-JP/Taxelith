import pandas as pd
import numpy as np
import io
import boto3
from botocore.exceptions import NoCredentialsError
from docx import Document
import PyPDF2
# import pymysql
from neo4j import GraphDatabase
from supabase import create_client, Client
from typing import List, Dict, Any, Optional
import tempfile
import os

s3 = boto3.client("s3")

def list_files_recursive(bucket_name = 'legaldocstorage', file_types = None) -> list[str]:

    paginator = s3.get_paginator("list_objects_v2")

    files = []
    for page in paginator.paginate(Bucket=bucket_name):
        for obj in page.get("Contents", []):
            files.append(obj["Key"])
    
    if file_types is None:
        return [f for f in files]
    else:
        return [f for f in files if f.lower().endswith(tuple(f".{ft}" for ft in file_types))]


def upload_file_to_s3(file_path: str, bucket_name: str = 'legaldocstorage', expire: int = 3600) -> str:
    """
    Upload a file to S3 and return a presigned URL.
    """
    
    object_name = file_path.split("/")[-1]
    if object_name is None:
        object_name = file_path.split("/")[-1]  # default: file name

    try:
        s3.upload_file(file_path, bucket_name, object_name)
        print(f"✅ Uploaded {file_path} to s3://{bucket_name}/{object_name}")

        # Generate presigned URL
        url = s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket_name, "Key": object_name},
            ExpiresIn=expire,
        )
        return f'{bucket_name}/{object_name}'
    except FileNotFoundError:
        raise Exception("❌ The file was not found")
    except NoCredentialsError:
        raise Exception("❌ AWS credentials not available")

def bucket_object_separator(bucket_object: str) -> tuple[str, str]:
    """
    Separates the bucket name and object name from a bucket object string.
    """
    bucket_name, object_name = bucket_object.split("/", 1)
    return bucket_name, object_name

def download_file_from_s3(bucket_object: str, local_path: str = ''):
    """
    Downloads a file from S3 and saves it locally.
    """
    bucket_name, object_name = bucket_object_separator(bucket_object)
    try:
        s3.download_file(bucket_name, object_name, local_path)
        print(f"✅ Downloaded s3://{bucket_name}/{object_name} to {local_path}")
    except NoCredentialsError:
        raise Exception("❌ AWS credentials not available")
    
def get_text_from_s3(bucket_object: str) -> str:
    """
    Fetches a file from S3 and returns parsed text.
    Supports: PDF, DOCX, JSON, CSV, TXT
    """
    bucket_name, object_name = bucket_object_separator(bucket_object)

    try:
        response = s3.get_object(Bucket=bucket_name, Key=object_name)
        body = response["Body"].read()
    except NoCredentialsError:
        raise Exception("❌ AWS credentials not available")

    # Guess file type by extension
    ext = object_name.split(".")[-1].lower()

    if ext == "pdf":
        reader = PyPDF2.PdfReader(io.BytesIO(body))
        return "\n".join(page.extract_text() or "" for page in reader.pages)

    elif ext == "docx":
        doc = Document(io.BytesIO(body))
        return "\n".join(p.text for p in doc.paragraphs)


    elif ext in ["txt", "log", "md"]:
        return body.decode("utf-8")

    else:
        raise Exception(f"❌ Unsupported file type: {ext}")

def download_s3_to_temp(bucket_object: str, suffix: str = None) -> str:
    """
    Download an S3 object to a temporary file and return the local path.

    Args:
        bucket_object (str): "bucket/key" format (e.g., "legaldocstorage/sample.pdf")
        suffix (str): Optional suffix (e.g., ".pdf"). If not provided, 
                      it will be inferred from the object name.

    Returns:
        str: Path to the temporary local file
    """
    bucket_name, object_name = bucket_object_separator(bucket_object)

    # infer suffix if not provided
    if suffix is None:
        ext = os.path.splitext(object_name)[-1]
        suffix = ext if ext else ".tmp"

    # create temp file
    tmp_file = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
    tmp_file.close()  # close so boto3 can write into it

    # download from S3
    download_file_from_s3(bucket_object, tmp_file.name)

    return tmp_file.name

#For AWS MySQL RDS
def dml_mysql(query:str):
    '''
    Function to interact with MySQL db hosted by AWS, used for DDL and DML only
    '''
    conn = pymysql.connect(
    host='',
    user='admin',
    password='',
    database='',
    port=3306,
    # ssl={'ca': r'D:/Study/Education/Projects/Group_Project/secrets/us-east-1-bundle.pem'}
    )
    
    cursor = conn.cursor()
    
    cursor.execute(query)
    
    conn.close()

def query_mysql(query:str):
    '''
    Function to query the MySQL db, used for Query and retrieving data
    '''
    conn = pymysql.connect(
    host='',
    user='admin',
    password='',
    database='',
    port=3306,
    # ssl={'ca': r'D:/Study/Education/Projects/Group_Project/secrets/us-east-1-bundle.pem'}
    )
    
    cursor = conn.cursor()

    cursor.execute(query)
    result = cursor.fetchall()

    conn.close()
    
    return result

#For neo4j
URI = "neo4j+s://4854812f.databases.neo4j.io"
AUTH = ("neo4j", "Y3J7Nh_bl6GgzcQH1SusosMAClb3g0fV0900dQ4f6aU")

def query_neo4j(query: str, **params):
    """
    Function to query the Neo4j db, used for Query and retrieving data
    """
    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        driver.verify_connectivity()
        records, summary, keys = driver.execute_query(
            query,
            **params,  #Expect params to be in dictionary format
            database="neo4j"
        )
        return [record.data() for record in records]


def dml_ddl_neo4j(query: str, **params):
    """
    Function for DDL and DML in Neo4j database
    """    
    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        driver.verify_connectivity()
        records, summary, keys = driver.execute_query(
            query,
            **params,   #Expect params to be in dictionary format
            database="neo4j"
        )

        print("Created {nodes_created} nodes, {rels_created} rels in {time} ms.".format(
            nodes_created=summary.counters.nodes_created,
            rels_created=summary.counters.relationships_created,
            time=summary.result_available_after
        ))

def save_to_txt(text: str, file_name: str):
    with open(f"D:/Study/Education/Projects/Group_Project/source/document/text_format/{file_name}.txt", "w", encoding="utf-8") as f:
        f.write(text)
    print("File saved as output.txt")