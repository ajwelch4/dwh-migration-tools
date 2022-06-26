# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Utility to upload to and download from GCS."""

import logging
import pathlib

from google.cloud import storage
from google.cloud.exceptions import NotFound
from google.cloud.storage import Bucket


def upload_directory(local_dir: pathlib.Path, bucket_name: str, gcs_path: str) -> None:
    """Uploads all the files from a local directory to a gcs bucket.

    Args:
      local_dir: path to the local directory.
      bucket_name: name of the gcs bucket.  If the bucket doesn't exist, the method
        tries to create one.
      gcs_path: the path to the gcs directory that stores the files.
    """
    if not local_dir.is_dir():
        raise ValueError(f"Can't find input directory {local_dir}.")
    client = storage.Client()

    try:
        logging.info("Get bucket %s", bucket_name)
        bucket: Bucket = client.get_bucket(bucket_name)
    except NotFound:
        logging.info('The bucket "%s" does not exist, creating one...', bucket_name)
        bucket = client.create_bucket(bucket_name)

    for local_path in local_dir.rglob("*"):
        if not local_path.is_file():
            continue
        relative_path = local_path.relative_to(local_dir)
        logging.info('Uploading file "%s" to gcs...', local_path)
        gcs_file_path = f"{gcs_path}/{relative_path}"
        blob = bucket.blob(gcs_file_path)
        blob.upload_from_filename(local_path)

    logging.info(
        'Finished uploading input files to gcs "%s/%s".', bucket_name, gcs_path
    )


def download_directory(
    local_dir: pathlib.Path, bucket_name: str, gcs_path: str
) -> None:
    """Download all the files from a gcs bucket to a local directory.

    Args:
        local_dir: path to the local directory to store the downloaded files. It will
            create the directory if it doesn't exist.
        bucket_name: name of the gcs bucket.
        gcs_path: the path to the gcs directory that stores the files.
    """
    client = storage.Client()
    blobs = client.list_blobs(bucket_name, prefix=gcs_path)
    logging.info('Start downloading outputs from gcs "%s/%s"', bucket_name, gcs_path)
    for blob in blobs:
        relative_path = pathlib.Path(blob.name).relative_to(gcs_path)
        local_path = local_dir / relative_path
        local_path.parent.mkdir(exist_ok=True)
        logging.info('Downloading output file to "%s"...', local_path)
        blob.download_to_filename(local_path.as_posix())

    logging.info('Finished downloading. Output files are in "%s".', local_dir)
