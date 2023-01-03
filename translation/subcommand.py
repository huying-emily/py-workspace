import click


@click.group()
def translation():
    pass


@translation.command()
@click.option("--mapper", "-m", required=True)
@click.option("--target_table", "-t", default=None, type=str, required=False)
@click.option("--source_table", "-s", default=None, type=str, required=False)
@click.option("--column_to_convert", "-c", default=None, type=str, required=False)
@click.option(
    "--column_with_simple_chinese", "-sc", default=None, type=str, required=False
)
@click.option(
    "--column_with_traditional_chinese", "-tc", default=None, type=str, required=False
)
@click.option("--ddl", "-ddl", default=None, type=str, required=False)
def execute(
    mapper: str,
    target_table: str = None,
    source_table: str = None,
    column_to_convert: str = None,
    column_with_simple_chinese: str = None,
    column_with_traditional_chinese: str = None,
    ddl: str = None,
):
    from src.translation.manager import TranslationManager

    tm = TranslationManager()
    tm.execute(
        mapper,
        target_table=target_table,
        source_table=source_table,
        column_to_convert=column_to_convert,
        column_with_simple_chinese=column_with_simple_chinese,
        column_with_traditional_chinese=column_with_traditional_chinese,
        ddl=ddl,
    )
