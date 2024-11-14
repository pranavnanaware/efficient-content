import streamlit as st
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
import math
import tempfile
import os
from tqdm import tqdm

AWS_ACCESS_KEY = st.secrets["AWS_ACCESS_KEY_ID"]
AWS_SECRET_KEY = st.secrets["AWS_SECRET_ACCESS_KEY_ID"]
AWS_REGION = "us-east-2"  # Replace with your desired region
S3_BUCKET = "efficient-content"  # Replace with your bucket name
CHUNK_SIZE = 5 * 1024 * 1024  # 5 MB

# Initialize S3 client


def init_s3_client():
    s3_client = boto3.client(
        's3',
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY
    )
    return s3_client

# Multipart upload function


def multipart_upload(file, s3_client, bucket, key, part_size=CHUNK_SIZE):
    try:
        # Initiate multipart upload
        mpu = s3_client.create_multipart_upload(Bucket=bucket, Key=key)
        upload_id = mpu['UploadId']
        parts = []
        part_number = 1

        # Get the size of the file
        file.seek(0, os.SEEK_END)
        file_size = file.tell()
        file.seek(0)
        total_parts = math.ceil(file_size / part_size)

        # Streamlit progress bar
        progress_bar = st.progress(0)
        status_text = st.empty()

        # Read and upload parts
        with tqdm(total=total_parts, desc="Uploading", unit="part") as pbar:
            while True:
                data = file.read(part_size)
                if not data:
                    break
                response = s3_client.upload_part(
                    Bucket=bucket,
                    Key=key,
                    PartNumber=part_number,
                    UploadId=upload_id,
                    Body=data
                )
                parts.append({
                    'ETag': response['ETag'],
                    'PartNumber': part_number
                })
                progress = part_number / total_parts
                progress_bar.progress(progress)
                status_text.text(
                    f"Uploading part {part_number} of {total_parts}")
                part_number += 1
                pbar.update(1)

        # Complete multipart upload
        s3_client.complete_multipart_upload(
            Bucket=bucket,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={'Parts': parts}
        )
        progress_bar.progress(1.0)
        status_text.text("Upload complete!")
        return True
    except ClientError as e:
        st.error(f"Client error: {e}")
        # Abort multipart upload in case of failure
        try:
            s3_client.abort_multipart_upload(
                Bucket=bucket, Key=key, UploadId=upload_id)
            st.info("Multipart upload aborted.")
        except Exception as abort_e:
            st.error(f"Failed to abort multipart upload: {abort_e}")
        return False
    except Exception as e:
        st.error(f"Unexpected error: {e}")
        return False

# Main Streamlit App


def main():
    st.title("Efficient - Content Upload")

    st.write(
        "Your content is safe with us. We respect your privacy and will never use your information without your consent. Thank you for contributing to our growth!")

    # File uploader
    uploaded_file = st.file_uploader("Choose a video file", type=[
                                     "mp4", "avi", "mov", "mkv"])

    if uploaded_file is not None:
        file_details = {
            "filename": uploaded_file.name,
            "filetype": uploaded_file.type,
            "filesize": uploaded_file.size
        }
        st.write(f"**Filename**: {file_details['filename']}")
        st.write(f"**File type**: {file_details['filetype']}")
        st.write(
            f"**File size**: {file_details['filesize'] / (1024 * 1024 * 1024):.2f} GB")

        # Validate file size
        if file_details['filesize'] > 5 * 1024 * 1024 * 1024:
            st.error("File size exceeds the 5 GB limit.")
            return

        # Confirm upload
        if st.button("Upload to S3"):
            with st.spinner("Uploading..."):
                s3_client = init_s3_client()
                # Use a temporary file to handle large uploads
                with tempfile.TemporaryFile() as tmp_file:
                    # Stream the uploaded file to the temporary file
                    st.info("Processing the file for upload...")
                    while True:
                        data = uploaded_file.read(CHUNK_SIZE)
                        if not data:
                            break
                        tmp_file.write(data)
                    # Reset file pointer to the beginning
                    tmp_file.seek(0)
                    # Update the uploaded_file object to the temporary file
                    uploaded_file = tmp_file

                    # Perform multipart upload
                    s3_key = f"videos/{file_details['filename']}"
                    success = multipart_upload(
                        uploaded_file, s3_client, S3_BUCKET, s3_key)
                    if success:
                        st.success(
                            f"File uploaded successfully to `s3://{S3_BUCKET}/{s3_key}`")
                    else:
                        st.error("File upload failed.")


if __name__ == "__main__":
    main()
