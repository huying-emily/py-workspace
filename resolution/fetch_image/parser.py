from src.utils.base_parser import BaseParser
from src.connector.manager import DatabaseManager
from rea_python.main.database import DBCopyMode
import boto3
import src.config as config
from rea_python.main.aws import S3Hook
import datetime
import uuid
import pandas as pd


class BaseImageParser(BaseParser):
    S3_IMAGE_BUCKET = None
    S3_IMAGE_FOLDER = None
    TARGET_TABLE = None
    DDL = None

    def __init__(self, dm: DatabaseManager):
        super().__init__(dm)
        # self.s3 = S3Hook(config.IMAGE_LAKE_S3_BUCKET)
        self.DDL = f"""
                begin;

                create table if not exists {self.TARGET_TABLE}
                (
                    s3_uri text,
                    obj_key text,
                    entity_tag text,
                    size_bytes integer,
                    s3_lastmodifiedtime text,
                    execution_date text,
                    data_uuid text
                );

                create index on {self.TARGET_TABLE} (s3_uri);

                commit;
            """

    def parse_images(self, execution_date: str, is_parse_all: bool):
        try:
            execution_date_value = datetime.datetime.strptime(
                execution_date, "%Y-%m-%d"
            ).date()
        except ValueError as e:
            raise Exception(f"Invalid execution_date parameter: {execution_date}")

        self.log.info(f"Execution date: {execution_date_value}")
        if is_parse_all:
            self.log.info("Searching all files regardless of s3 last-modified-time")
        else:
            self.log.info(f"Searching all files modified/created on {execution_date}")

        client = boto3.client("s3")
        operation_parameters = {
            "Bucket": self.S3_IMAGE_BUCKET,
            "Prefix": f"{self.S3_IMAGE_FOLDER}/",
            "PaginationConfig": {"pageSize": 100},
        }

        s3_bucket_path = "".join(
            ["s3://", self.S3_IMAGE_BUCKET + "/", self.S3_IMAGE_FOLDER + "/"]
        )
        self.log.info(f"Paginating {s3_bucket_path} for files...")

        df_list = []
        total_counter = 0
        processed_counter = 0

        for idx, img_key in enumerate(
            self._paginate(client.list_objects_v2, **operation_parameters)
        ):
            if not img_key["Key"].endswith("/"):
                total_counter += 1
                if total_counter % 500 == 0:
                    self.log.info(f"{total_counter} files scanned...")
                if is_parse_all or (
                    not is_parse_all
                    and img_key["LastModified"].date() == execution_date_value
                ):
                    img_dict = {
                        "s3_uri": "".join(
                            ["s3://", self.S3_IMAGE_BUCKET + "/", img_key["Key"]]
                        ),
                        "obj_key": img_key["Key"],
                        "entity_tag": img_key["ETag"].replace('"', ""),
                        "size_bytes": img_key["Size"],
                        "s3_lastmodifiedtime": str(img_key["LastModified"]),
                        "execution_date": execution_date,
                    }
                    df_list.append(pd.DataFrame.from_records([img_dict]))
                    processed_counter += 1
            else:
                self.log.warn(f'Skipping invalid key {img_key["Key"]}')

        self.log.info(
            f"{total_counter} file(s) found, {processed_counter} file(s) processed from {s3_bucket_path}"
        )

        if processed_counter > 0:
            final_df = pd.concat(df_list)
            final_df = self._add_uuid_to_df(final_df)
        else:
            final_df = pd.DataFrame(
                {
                    "s3_uri": [],
                    "obj_key": [],
                    "entity_tag": [],
                    "size_bytes": [],
                    "s3_lastmodifiedtime": [],
                    "execution_date": [],
                    "data_uuid": [],
                }
            )

        self.dm.pg_hook.copy_from_df(
            df=final_df,
            target_table=self.TARGET_TABLE,
            mode=DBCopyMode.DROP,
            ddl=self.DDL,
        )

        self.log.info(
            f"{final_df.shape[0]} record(s) persisted to postgres table: {self.TARGET_TABLE}"
        )

    def _add_uuid_to_df(self, df: "pd.DataFrame"):
        df["data_uuid"] = df.apply(lambda _: uuid.uuid4(), axis=1)
        return df

    def _paginate(self, method, **kwargs):
        client = boto3.client("s3")
        paginator = client.get_paginator(method.__name__)
        for page in paginator.paginate(**kwargs).result_key_iters():
            for result in page:
                yield result
