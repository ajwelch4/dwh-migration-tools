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
"""Helper class to upload to and download from GCS."""

import os
import uuid
from datetime import datetime
from os.path import abspath, basename, isdir, join

from google.cloud import storage
from google.cloud.exceptions import NotFound
from google.cloud.storage import Bucket


class GcsHelper:
    """Helper class to upload to and download from GCS."""

    def __init__(
        self,
        bucket: str,
        local_upload_path: str,
        local_download_path: str,
        translation_type: str,
    ) -> None:
        self.bucket = bucket
        self._path = self._generate_gcs_path(translation_type)
        self._local_upload_path = local_upload_path
        self.upload_path = os.path.join(self._path, "input")
        self._local_download_path = local_download_path
        self.download_path = os.path.join(self._path, "output")

    @staticmethod
    def _generate_gcs_path(translation_type: str) -> str:
        """Generates a gcs_path in the format of
        {translation_type}-{yyyy-mm-dd}-xxxx-xxxx-xxx-xxxx-xxxxxx.
        The suffix is a random generated uuid string.
        """
        return "%s-%s-%s" % (
            translation_type,
            datetime.now().strftime("%Y-%m-%d"),
            str(uuid.uuid4()),
        )

    def upload(self) -> None:
        """Uploads all the files from a local directory to a gcs bucket."""
        assert isdir(self._local_upload_path), (
            "Can't find input directory %s." % self._local_upload_path
        )
        client = storage.Client()

        try:
            print("Get bucket %s" % self.bucket)
            bucket: Bucket = client.get_bucket(self.bucket)
        except NotFound:
            print('The bucket "%s" does not exist, creating one...' % self.bucket)
            bucket = client.create_bucket(self.bucket)

        dir_abs_path = abspath(self._local_upload_path)
        for root, _, files in os.walk(dir_abs_path):
            for name in files:
                sub_dir = root[len(dir_abs_path) :]
                if sub_dir.startswith("/"):
                    sub_dir = sub_dir[1:]
                file_path = join(root, name)
                print('Uploading file "%s" to gcs...' % file_path)
                gcs_file_path = join(self.upload_path, sub_dir, name)
                blob = bucket.blob(gcs_file_path)
                blob.upload_from_filename(file_path)
        print(
            'Finished uploading input files to gcs "%s/%s".'
            % (self.bucket, self.upload_path)
        )

    def download(self) -> None:
        """Download all the files from a gcs bucket to a local directory."""
        client = storage.Client()
        blobs = client.list_blobs(self.bucket, prefix=self.download_path)
        print(
            'Start downloading outputs from gcs "%s/%s"'
            % (self.bucket, self.download_path)
        )
        for blob in blobs:
            file_name = basename(blob.name)
            sub_dir = blob.name[len(self.download_path) + 1 : -len(file_name)]
            file_dir = join(self._local_download_path, sub_dir)
            os.makedirs(file_dir, exist_ok=True)
            file_path = join(file_dir, file_name)
            print('Downloading output file to "%s"...' % file_path)
            blob.download_to_filename(file_path)

        print(
            'Finished downloading. Output files are in "%s".'
            % self._local_download_path
        )
