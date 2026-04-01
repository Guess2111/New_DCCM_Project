from typing import AnyStr, List
import openpyxl
import polars as pl
import pandas as pd
import narwhals as nw
import ipaddress
import regex as re
from narwhals.typing import IntoDataFrameT
from openpyxl import load_workbook


class SectionsWriter:
    def __init__(self, dest_workbook_path: str) -> None:
        self.dest_workbook_path = dest_workbook_path
    
    def writer(self, dict_tionary: dict) -> None:
        pass
    
    

class Prefix_section:
    def __init__(self) -> None:        
        self.section_column_mapping = {
                "prefix_set_name*":	"Prefix-list Name",
                "ip_subnet": "Prefix-IP"
        }
        
        self.section_required_columns = [
            "Action",
            "Sequence Action",
            "Version",
            "Prefix-list Name",
            "Sequence Number",
            "Permit/Deny",
            "Prefix-IP",
            "Length",
            "Route Policy Mapping",
            "Access-list Protocol",
            "Access-list Source IP",
            "Access-list source Wild Mask",
            "Access-list Destination IP",
            "Access-list destination Wild Mask"
        ]
        
    
    @nw.narwhalify
    def length_column_handler(self, df: IntoDataFrameT) -> List[AnyStr]:
        # 1. Define conditions for cleaner code
        has_eq = ~nw.col('expression_eq').is_null()
        has_ge = ~nw.col('expression_ge').is_null()
        has_le = ~nw.col('expression_le').is_null()

        # 2. Pre-cast to strings for safe concatenation
        str_eq = nw.col('expression_eq').cast(nw.String)
        str_ge = nw.col('expression_ge').cast(nw.String)
        str_le = nw.col('expression_le').cast(nw.String)

        # 3. Build the properly nested logic block
        formatted_expr = (
            nw.when(has_eq & (has_ge | has_le))
            .then(nw.lit("ERROR: Conflict between eq and ge/le"))
            .otherwise(
                nw.when(has_eq)
                .then(nw.lit("eq ") + str_eq)
                .otherwise(
                    nw.when(has_ge & has_le)
                    .then(nw.lit("ge ") + str_ge + nw.lit(" le ") + str_le)
                    .otherwise(
                        nw.when(has_ge)
                        .then(nw.lit("ge ") + str_ge)
                        .otherwise(
                            nw.when(has_le)
                            .then(nw.lit("le ") + str_le)
                            .otherwise(nw.lit(""))
                        )
                    )
                )
            )
        ).alias("Length")
        
        __processed_df = df.with_columns(formatted_expr)
        
        return __processed_df
    
    
    def get_ip_version(self, ip_value: str) -> str:
        if not isinstance(ip_value, str) or not ip_value.strip():
            return ""
        
        try:
            clean_ip = ip_value.strip().split('/')[0]
                        
            version = ipaddress.ip_address(clean_ip).version
            return f"ipv{version}"
        
        except ValueError:
            return ""
    
    
    @nw.narwhalify
    def get_the_version(self, df: IntoDataFrameT) -> IntoDataFrameT:
        col_name = 'ip_subnet'
        
        result_df = nw.to_native(df)
        
        if isinstance(result_df, pd.DataFrame):
            result_df['Version'] = result_df[col_name].fillna('').astype(str).str.strip().map(self.get_ip_version)
            
        if isinstance(result_df, pl.DataFrame):
            result_df = result_df.with_columns(
                pl.col(col_name).fill_null("").cast(pl.String).map_elements(self.get_ip_version, return_dtype=pl.String).alias("Version")
            )
        result_df = nw.from_native(result_df)
        return result_df
        
    
    @nw.narwhalify
    # def action_sequence_action_columns_handler(self, df: IntoDataFrameT) -> Tuple[List[AnyStr], List[AnyStr]]:
    def action_sequence_action_columns_handler(self, df: IntoDataFrameT) -> IntoDataFrameT:
        op_col = nw.col("Operation").cast(nw.String)
        
        is_target_op = op_col.str.to_lowercase().str.contains('add|delete')
        
        expr_a = nw.lit("A:") + op_col.str.to_titlecase()
        
        expr_b = nw.when(
            is_target_op
        ).then(
            nw.lit(
                "S:"
            ) + op_col.str.to_titlecase()
        ).otherwise(
            nw.lit("")
        )
        
        result_df = df.with_columns(
            expr_a.alias("Action"),
            expr_b.alias("Sequence Action")
        )
        
        return result_df
    
    
    def section_writer(self, worksheet: openpyxl.worksheet.worksheet.Worksheet, df: IntoDataFrameT):
        start_row = 1
        
        # worksheet = workbook_object["prefix"]
        max_row = worksheet.max_row
        max_col = worksheet.max_column
        # worksheet_df = None
        
        if max_row != 1 and max_col != 1:
            worksheet.delete_rows(1, max_row)
        
        max_row = worksheet.max_row
        max_col = worksheet.max_column
        
        neo_df = self.action_sequence_action_columns_handler(df)
        neo_df = self.length_column_handler(neo_df)
        neo_df = self.get_the_version(neo_df)
        
        keys_list = list(self.section_column_mapping.keys())
        
        for key in keys_list:
            value = self.section_column_mapping[key]
            neo_df= neo_df.with_columns(
                neo_df[key].alias(value),
            )
            
        columns = [column for column in list(neo_df.columns) if column in self.section_required_columns]
        neo_df = neo_df[columns]
        
        col_dict = {
            column: indx for indx, column in enumerate(self.section_required_columns, 1)
        }
        
        section_required_column_mapping_with_df = [col_dict[column] for column in columns]
        
        for column_name, col_idx in col_dict.items():
            worksheet.cell(row=start_row, column=col_idx, value=column_name)
        
        start_row += 1
        
        print(neo_df)
        for row_id in range(start_row, start_row + len(neo_df)):
            idx = row_id - start_row
            
            if str(neo_df[idx, 'Length']) == "ERROR: Conflict between eq and ge/le":
                for col_id in range(1, len(self.section_required_columns) + 1):
                    if col_id < 3:
                        col_name = self.section_required_columns[col_id - 1]
                        worksheet.cell(row=row_id, column=col_id, value=neo_df[idx, col_name])
                    
                    else:
                        worksheet.cell(row=row_id, column=col_id, value="")
                        
            else:
                for col_id in range(1, len(self.section_required_columns) + 1):
                    if col_id in section_required_column_mapping_with_df:
                        col_name = self.section_required_columns[col_id - 1]
                        worksheet.cell(row=row_id, column=col_id, value=neo_df[idx, col_name])
                        
                    
                    else:
                        worksheet.cell(row=row_id, column=col_id, value="")
                        
                        

class Policy_section:
    def __init__(self):
        pass
            