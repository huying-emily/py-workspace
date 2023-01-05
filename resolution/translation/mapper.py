import os
import numpy as np
import pandas as pd
import re
from rea_python.main.database import OutputFormat, DBCopyMode, read_sql
from src.utils.base_mapper import BaseMapper
from operator import xor
import chinese_converter as cc


class ChineseConverter(BaseMapper):
    def execute(self, **kwargs):

        source_table = kwargs["source_table"]
        target_table = kwargs["target_table"]
        column_to_convert = kwargs["column_to_convert"]
        ddl = kwargs["ddl"]
        if ddl == "None":
            pass
        elif ddl == "ddl":
            ddl_table = target_table.split("warehouse_")[1]
            ddlsql = read_sql(f"src/translation/ddl/{ddl_table}.sql")

        column_with_simple_chinese = kwargs["column_with_simple_chinese"]
        column_with_traditional_chinese = kwargs["column_with_traditional_chinese"]

        assert xor(
            bool(column_with_traditional_chinese), bool(column_with_simple_chinese)
        ), "You need to either pass a traditional Chinese column or a simple Chinese column waiting for conversion. "

        df = self.dm.rd_hook.execute_loaded_query(
            query_name="load_table_to_convert_chinese",
            output_format=OutputFormat.pandas,
            template_parameters={"source_table": source_table},
        )

        if column_with_simple_chinese:
            df_base = df[
                df[column_with_simple_chinese].notnull()
            ]  # To handle null values, leave those rows out
            df[column_to_convert] = df_base[column_with_simple_chinese].apply(
                lambda x: cc.to_traditional(x)
            )
            self.log.warning(f"Converting Simple Chinese to Traditional Chinese...")

        elif column_with_traditional_chinese:
            df_base = df[
                df[column_with_traditional_chinese].notnull()
            ]  # To handle null values, leave those rows out
            df[column_to_convert] = df_base[column_with_traditional_chinese].apply(
                lambda x: cc.to_simplified(x)
            )
            self.log.warning(f"Converting Traditional Chinese to Simple Chinese...")

        if ddl == "None":
            self.dm.rd_hook.copy_from_df(
                df=df, target_table=target_table, mode=DBCopyMode.DROP
            )
        elif ddl == "ddl":
            self.dm.rd_hook.copy_from_df(
                df=df, target_table=target_table, mode=DBCopyMode.DROP, ddl=ddlsql
            )
        self.log.warning(
            f"finish converting between Simple Chinese and Traditional Chinese..."
        )
