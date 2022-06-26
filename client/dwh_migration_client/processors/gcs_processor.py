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
"""Processor to upload to and download from GCS."""

import logging
import pathlib
import uuid
from datetime import datetime

from google.cloud import storage
from google.cloud.exceptions import NotFound
from google.cloud.storage import Bucket

from dwh_migration_client.processors.abstract_processor import Processor


class GcsProcessor(Processor):
    """Processor to upload to and download from GCS."""

    def __init__(
        self,
        bucket: str,
    ) -> None:
        self._bucket = bucket
        self._gcs_path = pathlib.Path(f"{datetime.now().isoformat()}-{uuid.uuid4()}")
        self._upload_path = f"{self._gcs_path}/input"
        self.upload_uri = f"gs://{self._bucket}/{self._upload_path}"
        self._download_path = f"{self._gcs_path}/output"
        self.download_uri = f"gs://{self._bucket}/{self._download_path}"

    def preprocess(self, relative_path: pathlib.Path, text: str) -> str:
        """Upload preprocessed text to GCS."""
        client = storage.Client()
        logging.debug("Get bucket %s", self._bucket)
        try:
            bucket: Bucket = client.get_bucket(self._bucket)
        except NotFound:
            logging.debug(
                'The bucket "%s" does not exist, creating one...', self._bucket
            )
            bucket = client.create_bucket(self._bucket)
        blob = bucket.blob(f"{self._upload_path}/{relative_path}")
        logging.info("Uploading to GCS: %s/%s.", self.upload_uri, relative_path)
        blob.upload_from_string(text)
        logging.info(
            "Finished uploading to GCS: %s.",
            blob.self_link,
        )
        return text

    def postprocess(self, relative_path: pathlib.Path, text: str) -> str:
        """Download translated text from GCS."""
        client = storage.Client()
        bucket: Bucket = client.get_bucket(self._bucket)
        blob = bucket.blob(f"{self._download_path}/{relative_path}")
        logging.info("Downloading from GCS: %s/%s.", self.download_uri, relative_path)
        translated_text: str = blob.download_as_text()
        logging.info(
            "Finished downloading from GCS: %s/%s.", self.download_uri, relative_path
        )
        return translated_text
