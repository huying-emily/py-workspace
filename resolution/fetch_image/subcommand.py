import click
import datetime


@click.group()
def fetch_image():
    pass


@fetch_image.command()
@click.option("--entity_name", "-e")
def import_image(entity_name: str):
    """
    import image id and image_type into masterdata fc table
    """
    from src.fetch_image.manager import FetchImageManager

    m = FetchImageManager()
    m.raw_import(entity_name)


@fetch_image.command()
@click.option("--entity_name", "-e")
def download(entity_name: str):
    """
    fetch the image for the image related entities
    """
    from src.fetch_image.manager import FetchImageManager

    m = FetchImageManager()
    m.execute(entity_name)


@fetch_image.command()
@click.option("--entity_name", "-e")
@click.option("--s3_obj_key_column_name", "-c", required=False, type=str, default=None)
def back_fill(entity_name: str, s3_obj_key_column_name: str):
    """
    combine import and download into single command line for production pipeline
    """
    from src.fetch_image.manager import FetchImageManager

    m = FetchImageManager()
    m.back_fill(entity_name, s3_obj_key_column_name)


@fetch_image.command()
@click.option("--entity_name", "-e")
@click.argument("execution_date", type=str)
@click.option("--parse_all", "-a", required=False, type=bool, default=True)
def parse_image(entity_name: str, execution_date: str, parse_all: bool):
    """Iterates through files in S3 location and parse all metadata into a specified postgres table"""
    from src.fetch_image.manager import ParseImageManager

    try:
        datetime.datetime.strptime(execution_date, "%Y-%m-%d")
    except ValueError as e:
        raise Exception(f"Invalid execution_date parameter: {execution_date}")

    p = ParseImageManager()
    p.parse_images(
        target=entity_name, execution_date=execution_date, is_parse_all=parse_all
    )
