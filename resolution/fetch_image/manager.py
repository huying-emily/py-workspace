from src.utils.base_manager import BaseManager
import src.fetch_image.mapper as fim
import src.fetch_image.parser as fip


class FetchImageManager(BaseManager):
    """
    this manager class handles the following logic

    1. read from our existing normalised schema, all image urls
    2. fetch them (download them) and upload them back to S3 bucket
    3. insert the S3 links back to the masterdata FC table

    """

    REGISTRY = {
        "project_listing_image": fim.ProjectImageFetcher,
        "sale_listing_image": fim.SaleImageFetcher,
        "rental_listing_image": fim.RentalImageFetcher,
        "sg_condoreview_project_image": fim.CondoReviewProjectImageFetcher,
        "sg_cos_project_image": fim.CosProjectImageFetcher,
        "sg_quadrant_images": fim.QuadrantImageFetcher,
        "sg_external_project_image": fim.SGExternalProjectImageFetcher,
        "sg_new_launch_image": fim.SGNewLaunchImageFetcher,
        "my_new_launch_image": fim.MYNewLaunchImageFetcher,
        "id_new_launch_image": fim.IDNewLaunchImageFetcher,
        "hk_external_project_image": fim.HKExternalProjectImageFetcher,
    }

    def raw_import(self, target_table: str):
        mapper = self._load_mapper(target_table)
        mapper(self.dm).raw_import()

    def execute(self, target_table: str):
        mapper = self._load_mapper(target_table)
        mapper(self.dm).map()

    def back_fill(self, target_table: str, s3_obj_key_column_name: str):
        mapper = self._load_mapper(target_table)
        mapper(self.dm).back_fill(s3_obj_key_column_name)


class ParseImageManager(BaseManager):
    REGISTRY = {
        "sg_quadrant_images": fip.QuadrantImageParser,
        "sg_external_project_image": fip.SGExternalProjectImageParser,
        "hk_external_project_image": fip.HKExternalProjectImageParser,
    }

    def parse_images(self, target: str, execution_date: str, is_parse_all: bool):
        parser = self._load_parser(target)
        parser(self.dm).parse_images(
            execution_date=execution_date, is_parse_all=is_parse_all
        )
