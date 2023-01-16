from src.utils.base_mapper import BaseMapper
from rea_python.main.database import PostgresHook, OutputFormat, DBCopyMode
import requests
import boto3
import src.config as config
import src.constants as constants
import io
import pandas as pd
from tqdm import tqdm
from rea_python.main.aws import S3Hook
from src.connector.manager import DatabaseManager


class BaseImageFetcher(BaseMapper):
    """
    the mapper class handles the following logic:
    1. read from our existing normalised schema, all image urls
    2. fetch them (download them) and upload them back to S3 bucket
    3. insert the S3 links back to the masterdata FC table
    """

    image_columns = ["origin_term", "image_type", "s3_full_link"]
    src_schema = ""
    star_entity_name = ""
    master_fc_name = ""
    table_cols = ""
    data_source = ""
    metadata = ""
    url_column = ""
    image_type_column = ""
    is_direct_import = False
    data = []

    S3_IMAGE_FOLDER = None

    def __init__(self, dm: DatabaseManager):
        super().__init__(dm)
        self.s3 = S3Hook(config.IMAGE_S3_BUCKET)

    def _cached_update_unknown(self, item):
        full_url = "UNKNOWN"
        single_record = [item.origin_term, item.image_type, full_url]
        return self.data.append(single_record)

    def raw_import(self, s3_obj_key_column_name):
        self.log.warning(
            f"Mapping import data process start. Using mapper: {self.star_entity_name}"
        )
        # import star image data into masterdata fc table
        if len(self.data_source) == 0:
            self.dm.pg_hook.execute_loaded_query(
                query_name="raw_import_from_star",
                template_parameters={
                    "src_schema": self.src_schema,
                    "star_entity_name": self.star_entity_name,
                    "master_fc_name": self.master_fc_name,
                },
            )
        else:
            self.log.info(f"Data-source: {self.data_source}, metadata: {self.metadata}")
            if s3_obj_key_column_name is None:
                self.dm.pg_hook.execute_loaded_query(
                    query_name="raw_import_by_data_source",
                    template_parameters={
                        "src_schema": self.src_schema,
                        "entity_name": self.star_entity_name,
                        "master_fc_name": self.master_fc_name,
                        "data_source_value": self.data_source,
                        "metadata_value": self.metadata,
                    },
                )
            else:
                self.log.info(f"S3 obj_key column name: {s3_obj_key_column_name}")
                self.dm.pg_hook.execute_loaded_query(
                    query_name="raw_direct_import_by_data_source",
                    template_parameters={
                        "src_schema": self.src_schema,
                        "entity_name": self.star_entity_name,
                        "master_fc_name": self.master_fc_name,
                        "data_source_value": self.data_source,
                        "metadata_value": self.metadata,
                        "s3_url_prefix": constants.S3_IMAGE_OBJECT_URL_PREFIX,
                        "obj_key_column": s3_obj_key_column_name,
                        "image_type_column": self.image_type_column,
                    },
                )

    def map(self):
        if self.is_direct_import:
            self.log.warning(
                f"Mapper {self.star_entity_name} is direct-import, skipping image mapping"
            )
            return
        else:
            self.log.warning(
                f"Mapping resolution process start. Using mapper: {self.star_entity_name}"
            )

        # join with star to get image original_link
        resp = self.dm.pg_hook.execute_loaded_query(
            query_name="get_new_images",
            template_parameters={
                "src_schema": self.src_schema,
                "star_entity_name": self.star_entity_name,
                "master_fc_name": self.master_fc_name,
                "url_column": self.url_column,
            },
        )

        # check whether the link is present, if so, re apply association
        files = self.s3.list_files_in_bucket(prefix=self.S3_IMAGE_FOLDER)

        # download the image and upload the binary file to s3 bucket
        if len(resp.all()) == 0:
            self.log.warning(f"No image needs to be fetched...")
        else:
            for item in tqdm(resp):

                s3_prefix = (
                    f"{self.S3_IMAGE_FOLDER}/{item.image_type}/{item.origin_term}"
                )

                if s3_prefix in files:
                    self._cached_update_s3_link(item, s3_prefix)
                    continue

                file_name = item.original_link

                try:
                    file_as_binary = requests.get(file_name, stream=True)
                except requests.exceptions.ConnectionError:
                    self._cached_update_unknown(item)
                    continue

                if file_as_binary.status_code == 200:

                    # try except removed
                    self.s3.upload_file(
                        target_key=s3_prefix,
                        target_binary=io.BytesIO(file_as_binary.content),
                    )

                    self._cached_update_s3_link(item, s3_prefix)
                else:
                    self._cached_update_unknown(item)

        final_df = pd.DataFrame(data=self.data, columns=self.image_columns)

        # prepare table in DB
        temp_table = f"masterdata.temp_{self.master_fc_name}"
        self.dm.pg_hook.copy_from_df(
            df=final_df, target_table=temp_table, mode=DBCopyMode.DROP
        )

        # update fc table s3_link
        if len(final_df) != 0:
            self.dm.pg_hook.execute_loaded_query(
                query_name="update_fc_image",
                template_parameters={
                    "master_fc_name": self.master_fc_name,
                    "temp_table": temp_table,
                },
            )
        else:
            self.log.warning("No Image link need to be updated, exiting...")

        # clean temp table
        self.dm.pg_hook.execute_raw_query(f"DROP TABLE {temp_table} CASCADE;")

        self.log.warning("Image fetching process completed. ")

    def _cached_update_s3_link(self, item, s3_prefix):
        full_url = f"{constants.S3_IMAGE_OBJECT_URL_PREFIX}/{s3_prefix}"
        single_record = [item.origin_term, item.image_type, full_url]
        self.log.info(f"single_record: {single_record}")
        self.data.append(single_record)

    def back_fill(self, s3_obj_key_column_name):
        # combine import and download into single command line for production pipeline
        self.raw_import(s3_obj_key_column_name)
        # self.log.info(f"Skipping map for now")
        self.map()


class SGNewLaunchImageFetcher(BaseImageFetcher):
    src_schema = "reference"
    star_entity_name = "sg_new_launch_image"
    data_source = "sg_new_launch_image"
    metadata = "project_name"
    # master_fc_name = "fc_project_listing_image"
    url_column = "image_url"
    S3_IMAGE_FOLDER = "project_image_v2"

    def map(self):
        if self.is_direct_import:
            self.log.warning(
                f"Mapper {self.star_entity_name} is direct-import, skipping image mapping"
            )
            return
        else:
            self.log.warning(
                f"Mapping resolution process start. Using mapper: {self.star_entity_name}"
            )

        # join with star to get image original_link
        resp = self.dm.pg_hook.execute_loaded_query(
            query_name="get_new_images_fr_reference",
            template_parameters={
                "src_schema": self.src_schema,
                "star_entity_name": self.star_entity_name,
                # "master_fc_name": self.master_fc_name,
                "url_column": self.url_column,
            },
        )

        # check whether the link is present, if so, re apply association
        files = self.s3.list_files_in_bucket(prefix=self.S3_IMAGE_FOLDER)

        # download the image and upload the binary file to s3 bucket
        if len(resp.all()) == 0:
            self.log.warning(f"No image needs to be fetched...")
        else:
            # self.log.warning(f"There are {len(resp.all())} images to be fetched... ")
            for item in tqdm(resp):

                s3_prefix = (
                    f"{self.S3_IMAGE_FOLDER}/{item.image_type}/{item.origin_term}"
                )

                if s3_prefix in files:
                    # self.log.info("Image exists in s3. continue... ")
                    self._cached_update_s3_link(item, s3_prefix)
                    continue

                file_name = item.original_link
                self.log.info(f'file_name: {file_name}')
                try:
                    file_as_binary = requests.get(file_name, stream=True)  # , verify=False
                except requests.exceptions.ConnectionError:
                    self._cached_update_unknown(item)
                    continue

                if file_as_binary.status_code == 200:

                    # try except removed
                    self.s3.upload_file(
                        target_key=s3_prefix,
                        target_binary=io.BytesIO(file_as_binary.content),
                    )

                    self._cached_update_s3_link(item, s3_prefix)
                else:
                    self.log.warning("request status code not 200. ")
                    self._cached_update_unknown(item)

        final_df = pd.DataFrame(data=self.data, columns=self.image_columns)

        # prepare table in DB
        temp_table = f"reference.temp_{self.star_entity_name}"
        self.dm.pg_hook.copy_from_df(
            df=final_df, target_table=temp_table, mode=DBCopyMode.DROP
        )
        # update fc table s3_link
        if len(final_df) != 0:
            self.dm.pg_hook.execute_loaded_query(
                query_name="update_reference_image",
                template_parameters={
                    "src_schema": self.src_schema,
                    "star_entity_name": self.star_entity_name,
                    # "master_fc_name": self.master_fc_name,
                    "temp_table": temp_table,
                },
            )
        else:
            self.log.warning("No Image link need to be updated, exiting...")

        # clean temp table
        self.dm.pg_hook.execute_raw_query(f"DROP TABLE {temp_table} CASCADE;")

        self.log.warning("Image fetching process completed. ")
