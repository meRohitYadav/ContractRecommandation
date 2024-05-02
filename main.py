from fastapi import FastAPI, File, UploadFile, Response
from typing import List
from langchain_text_splitters import TokenTextSplitter
from docx import Document
from PyPDF2 import PdfReader
from openai import OpenAI
from dotenv import load_dotenv
import chromadb
import uuid
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, generate_blob_sas, BlobSasPermissions
import os
from fastapi.responses import StreamingResponse
from datetime import timedelta, datetime

load_dotenv()

app = FastAPI()
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

openai_client = OpenAI()
chroma_client = chromadb.HttpClient(host=os.getenv("CHROMADB_HOST"), port=os.getenv("CHROMADB_PORT"))
container_client = ContainerClient(account_url=os.getenv("AZURE_STORAGE_ACCOUNT_URL"), container_name=os.getenv("AZURE_STORAGE_CONTAINER_NAME"), credential=os.getenv("AZURE_STORAGE_CREDENTIAL"))

def get_embedding(text, model="text-embedding-3-large"):
    text = text.replace("\n", " ")
    return openai_client.embeddings.create(input = [text], model=model).data[0].embedding


def extract_text_from_pdf(file: UploadFile) -> str:
    """
    Extracts text from the uploaded PDF file using PyPDF2.
    """
    reader = PdfReader(file.file)

    file_text = ""
    for page in reader.pages:
        file_text += page.extract_text()
    
    return file_text

def extract_text_from_docx(docx_file):
    """
    Extracts text from a .docx file.
    """
    doc = Document(docx_file.file)
    full_text = ""
    for para in doc.paragraphs:
        full_text += para.text
    return full_text

def generate_random_uuids_for_chunks(n):
    random_uuids = [uuid.uuid4() for _ in range(n)]
    prefixed_uuids = ["chunk-" + str(uuid_) for uuid_ in random_uuids]
    return prefixed_uuids

def generate_random_uuid_for_contract():
    return "contract-"+ str(uuid.uuid4()) 

def transform_chunks_parent_name(string_array):
    objects_array = []
    for string in string_array:
        obj = {"parent_name": string}
        objects_array.append(obj)
    return objects_array

def create_service_sas_blob(blob_client: BlobClient, account_key: str, file_path: str):
    # Create a SAS token that's valid for one day, as an example

    expiry_time = datetime.utcnow() + timedelta(days=10)

    sas_token = generate_blob_sas(
        account_name=blob_client.account_name,
        container_name=blob_client.container_name,
        blob_name=file_path,
        account_key=account_key,
        permission=BlobSasPermissions(read=True),
        expiry=expiry_time
    )
    return sas_token


@app.post("/upload/")
async def upload_files(files: List[UploadFile] = File(...)):
    
    for file in files:
        if not file.filename.endswith(".pdf") and not file.filename.endswith(".docx"):
            return "Please provide either .docx file or .pdf file"
    
    # blob_service_client = get_blob_service_client_account_key()
    
    extracted_texts = []
    filenames = []
    for file in files:
        filename = generate_random_uuid_for_contract()
        filenames.append(filename)
        container_client.upload_blob(name=filename, data=file.file)
        if file.filename.endswith(".pdf"):
            text = extract_text_from_pdf(file)
            extracted_texts.append(text)
        elif file.filename.endswith(".docx"):
            text = extract_text_from_docx(file)
            extracted_texts.append(text)

    chunks = []
    parent_for_chunks = []
    text_splitter = TokenTextSplitter(chunk_size=6000, chunk_overlap=0)

    for index, item in enumerate(extracted_texts):
        splittedText = text_splitter.split_text(item)
        parent_for_chunks.extend([ filenames[index]]*len(splittedText) )
        chunks.extend(splittedText)

    embeddings = []
    for chunk in chunks:
        embeddings.append(get_embedding(chunk))

    collection = chroma_client.get_or_create_collection(name="contracts")

    collection.add(
        embeddings=embeddings,
        documents=chunks,
        ids = generate_random_uuids_for_chunks(len(chunks)),
        metadatas= transform_chunks_parent_name(parent_for_chunks)
    )

    return "success"



@app.post("/getRelatedContracts/")
async def upload_file(response: Response, file: UploadFile = File(...)):
    # blob_service_client = BlobServiceClient(account_url=f"https://contractrecommandstorage.blob.core.windows.net", credential="GrkhQNhDWx+nE+je0zWYy/x5sbAYRbp58+qgtC9bYqdayGEdJ4INWTzEkb632wTSLzaS7vdobu0/+AStB6wEig==")
    
    # deal_file_path = "/contracts/contract-a24f35db-7b48-4b1c-a760-ae9ccc217854"
    # container_name = "contracts"
        
    # blob_client = blob_service_client.get_blob_client(container=container_name, blob=deal_file_path)
    # expiry_time = datetime.utcnow() + timedelta(days=10)

    # sas_token = generate_blob_sas(
    #         blob_client.account_name,
    #         container_name,
    #         deal_file_path,
    #         account_key=blob_service_client.credential.account_key,
    #         permission=BlobSasPermissions(read=True),
    #         expiry=expiry_time,
    # )

    # return sas_token

    if not file.filename.endswith(".pdf") and not file.filename.endswith(".docx"):
            return "Please provide either .docx file or .pdf file"

    extracted_text = ""
    if file.filename.endswith(".pdf"):
        extracted_text = extract_text_from_pdf(file)
    elif file.filename.endswith(".docx"):
        extracted_text = extract_text_from_docx(file)

    chunks = []
    text_splitter = TokenTextSplitter(chunk_size=6000, chunk_overlap=0)
    splittedText = text_splitter.split_text(extracted_text)
    chunks.extend(splittedText)

    embeddings = []
    for chunk in chunks:
        embeddings.append(get_embedding(chunk))


    collection = chroma_client.get_or_create_collection(name="contracts")

    query_result = collection.query(
    query_embeddings=embeddings,
    n_results=5,
    )

    related_contracts_names = []
    related_contracts = []
    for metadata in query_result["metadatas"][0]:
        related_contracts_names.append(metadata["parent_name"])
        related_contracts.append( container_client.download_blob(blob=metadata["parent_name"]).readall() )
    
    # blob_service_client = container_client.get_blob_client("contract-a24f35db-7b48-4b1c-a760-ae9ccc217854")
    # return os.getenv("AZURE_STORAGE_ACCOUNT_URL") + "/" + os.getenv("AZURE_STORAGE_CONTAINER_NAME") + "/" + "contract-a24f35db-7b48-4b1c-a760-ae9ccc217854" + "?" + create_service_sas_blob(blob_service_client, os.getenv("AZURE_STORAGE_CREDENTIAL"), "/contracts/contract-a24f35db-7b48-4b1c-a760-ae9ccc217854") 
    
    response.headers["Content-Disposition"] = f"attachment; filename={related_contracts_names[0]}"
    return Response(content=related_contracts[0], media_type="application/octet-stream")



    
