from fastapi import FastAPI, File, UploadFile
from typing import List
from langchain_text_splitters import TokenTextSplitter
from docx import Document
from PyPDF2 import PdfReader
from openai import OpenAI
from dotenv import load_dotenv
import chromadb
import uuid
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import os

load_dotenv()

app = FastAPI()

openai_client = OpenAI()
chroma_client = chromadb.HttpClient(host=os.getenv("CHROMADB_HOST"), port=os.getenv("CHROMADB_PORT"))

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

@app.post("/upload/")
async def upload_files(files: List[UploadFile] = File(...)):
    
    for file in files:
        if not file.filename.endswith(".pdf") and not file.filename.endswith(".docx"):
            return "Please provide either .docx file or .pdf file"
    
    # blob_service_client = get_blob_service_client_account_key()
    container_client = ContainerClient(account_url=os.getenv("AZURE_STORAGE_ACCOUNT_URL"), container_name=os.getenv("AZURE_STORAGE_CONTAINER_NAME"), credential=os.getenv("AZURE_STORAGE_CREDENTIAL"))
    
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

    return collection.get()




