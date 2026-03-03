import os
import pandas as pd
from typing import List, Optional
import openpyxl
from openpyxl import Workbook, load_workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter
from messages import Messagebox


class ExcelModifier:
    def __init__(
        self,
        workbook_path,
        sheet_name: str,
        dataframe: Optional[pd.DataFrame] = None,
        index_required: Optional[bool] = False,
        wrap_text: Optional[bool] = False,
    ):
        self.workbook = None
        self.workbook_to_be_saved = workbook_path

        if dataframe is not None:
            excel_writer = None
            if os.path.exists(workbook_path):
                excel_writer = pd.ExcelWriter(
                    workbook_path,
                    engine="openpyxl",
                    mode="a",
                    if_sheet_exists="replace",
                )

            else:
                excel_writer = pd.ExcelWriter(
                    workbook_path,
                    engine="openpyxl",
                    mode="w",
                    if_sheet_exists="replace",
                )

            if excel_writer:
                dataframe.to_excel(
                    excel_writer, sheet_name=sheet_name, index=index_required
                )
                excel_writer.save()
                excel_writer.close()
                del excel_writer

        self.worksheet = None
        self.columns = None
        self.rows = None
        self.header_row = None
        self.header_column = None

        self.side = Side(style="medium", color="000000")
        self.border = Border(
            left=self.side, right=self.side, top=self.side, bottom=self.side
        )
        self.fill = None
        self.alignment = Alignment(
            horizontal="center", vertical="center", wrap_text=wrap_text
        )
        self.header_font = Font(
            name="Ericsson Hilda", bold=True, size=14, color="FFFFFF"
        )
        self.normal_font = Font(name="Ericsson Hilda", size=11)

        if os.path.exists(workbook_path):
            self.workbook = load_workbook(workbook_path, read_only=False)
            self.worksheet = self.workbook[sheet_name]
            self.columns = self.worksheet.max_column
            self.rows = self.worksheet.max_row
            self.header_row, self.header_column = self.first_row_finder_for_header()

    def first_row_finder_for_header(self):
        i = 1
        j = 1
        breaker = False
        while i <= self.rows:
            while j <= self.columns:
                if self.worksheet.cell(row=i, column=j).value is not None:
                    header_row = i
                    header_column = j
                    breaker = True
                    break
                j += 1
            if breaker:
                break
            i += 1
        return header_row, header_column

    def column_width_adjuster(self):
        col_width = []
        default_font_size = 11

        i = self.header_row
        # print(f"{self.rows =}")

        total_columns = self.columns - self.header_column + 1

        while i <= self.rows:
            # print(f"{i = }")
            j = self.header_column
            while j <= self.columns:
                # print(f"{j = }")
                # print(f"{str(self.worksheet.cell(row=i, column=j).value) = }")

                cell_content_size = len(str(self.worksheet.cell(row=i, column=j).value).strip())
                cell = self.worksheet.cell(row=i, column=j)
                current_font_size = cell.font.size if cell.font and cell.font.size else default_font_size
                scaling_factor = current_font_size / default_font_size
                bold_multiplier = 1.45 if cell.font and cell.font.bold else 1.3
                required_size = (cell_content_size * scaling_factor * bold_multiplier)
                # print(f"{required_size =}")

                if len(col_width) < total_columns:

                    col_width.append(
                        required_size
                    )
                else:
                    list_index_to_be_updated = self.columns - (total_columns + j)
                    # print(f"{list_index_to_be_updated =}")
                    col_width[list_index_to_be_updated] = min(
                        max(col_width[list_index_to_be_updated], required_size), 50
                    )
                    # print(f"{col_width[list_index_to_be_updated] =}")

                j += 1
            i += 1

        i = 0
        while i < len(col_width):
            if col_width[i] < 50:
                col_width[i] += 3
            i += 1

        j = self.header_column
        while j <= self.columns:
            self.worksheet.column_dimensions[get_column_letter(j)].width = col_width[
                j - self.header_column
            ]
            j += 1

    def normal_styler(self):
        self.fill = PatternFill(
            start_color="3333FF", end_color="3333FF", fill_type="solid"
        )
        i = self.header_column
        while i <= self.columns:
            self.worksheet.cell(row=self.header_row, column=i).fill = self.fill
            self.worksheet.cell(
                row=self.header_row, column=i
            ).alignment = self.alignment
            self.worksheet.cell(row=self.header_row, column=i).font = self.header_font
            self.worksheet.cell(row=self.header_row, column=i).border = self.border
            i += 1

        i = self.header_row + 1
        while i <= self.rows:
            j = self.header_column
            while j <= self.columns:
                self.worksheet.cell(row=i, column=j).alignment = self.alignment
                self.worksheet.cell(row=i, column=j).border = self.border
                self.worksheet.cell(row=i, column=j).font = self.normal_font
                j += 1
            i += 1

        self.column_width_adjuster()
        self.save()

    def special_styler(self):
        pass

    def merger(self, range: List = None):
        pass
    
    @property
    def get_openpyxl_workbook(self)->Workbook|None:
        return self.workbook

    def save(self):
        if self.workbook:
            self.workbook.save(self.workbook_to_be_saved)
            self.workbook.close()
            del self.workbook
            
            

class ExcelReader:
    def __init__(self, workbook_path: str):
        self.workbook_path = workbook_path
        self.workbook_load = None
        self.sheets = None
        
    @property
    def get_sheets(self) -> List[str]|None:
        if os.path.exists(self.workbook_path):
            self.workbook_load = load_workbook(self.workbook_path, read_only=False)
            self.sheets = self.workbook_load.sheetnames
            return self.sheets
        else:
            messagebox = Messagebox()
            messagebox.showerror("File Not Found", "File Not Found!")
            return None
    
    def get_openpyxl_workbook(self)->Workbook|None:
        return self.workbook_load

    def save(self):
        if self.workbook_load:
            self.workbook_load.save(self.workbook_path)
            self.quit()
            
    def quit(self):
        if self.workbook_load:
            self.workbook_load.close()
            del self.workbook_load
            

class ExcelSheetModifier:
    def __init__(self, ws: openpyxl.worksheet.worksheet.Worksheet):
        self.worksheet = ws
        self.header_row = None
        self.header_column = None
        self.columns = None
        self.rows = None
        
        self.columns = self.worksheet.max_column
        self.rows = self.worksheet.max_row
        self.header_row, self.header_column = self.first_row_finder_for_header()
    

    @property
    def sheet(self)->openpyxl.worksheet.worksheet.Worksheet:
        return self.worksheet
    
    
    def first_row_finder_for_header(self):
        i = 1
        j = 1
        breaker = False
        while i <= self.rows:
            while j <= self.columns:
                if self.worksheet.cell(row=i, column=j).value is not None:
                    header_row = i
                    header_column = j
                    breaker = True
                    break
                j += 1
            if breaker:
                break
            i += 1
        return header_row, header_column

    def column_width_adjuster(self):
        col_width = []
        default_font_size = 11

        i = self.header_row
        # print(f"{self.rows =}")

        total_columns = self.columns - self.header_column + 1

        while i <= self.rows:
            # print(f"{i = }")
            j = self.header_column
            while j <= self.columns:
                # print(f"{j = }")
                # print(f"{str(self.worksheet.cell(row=i, column=j).value) = }")

                cell_content_size = len(str(self.worksheet.cell(row=i, column=j).value).strip())
                cell = self.worksheet.cell(row=i, column=j)
                current_font_size = cell.font.size if cell.font and cell.font.size else default_font_size
                scaling_factor = current_font_size / default_font_size
                bold_multiplier = 1.45 if cell.font and cell.font.bold else 1.3
                required_size = (cell_content_size * scaling_factor * bold_multiplier)
                # print(f"{required_size =}")

                if len(col_width) < total_columns:

                    col_width.append(
                        required_size
                    )
                else:
                    list_index_to_be_updated = self.columns - (total_columns + j)
                    # print(f"{list_index_to_be_updated =}")
                    col_width[list_index_to_be_updated] = min(
                        max(col_width[list_index_to_be_updated], required_size), 50
                    )
                    # print(f"{col_width[list_index_to_be_updated] =}")

                j += 1
            i += 1

        i = 0
        while i < len(col_width):
            if col_width[i] < 50:
                col_width[i] += 3
            i += 1

        j = self.header_column
        while j <= self.columns:
            self.worksheet.column_dimensions[get_column_letter(j)].width = col_width[
                j - self.header_column
            ]
            j += 1

    def normal_styler(self, wrap_text: bool = False):
        self.side = Side(style="medium", color="000000")
        self.border = Border(
            left=self.side, right=self.side, top=self.side, bottom=self.side
        )
        self.fill = None
        self.alignment = Alignment(
            horizontal="center", vertical="center", wrap_text=wrap_text
        )
        self.header_font = Font(
            name="Ericsson Hilda", bold=True, size=14, color="FFFFFF"
        )
        self.normal_font = Font(name="Ericsson Hilda", size=11)
        self.wrap_text = wrap_text
        self.fill = PatternFill(
            start_color="3333FF", end_color="3333FF", fill_type="solid"
        )
        i = self.header_column
        while i <= self.columns:
            self.worksheet.cell(row=self.header_row, column=i).fill = self.fill
            self.worksheet.cell(
                row=self.header_row, column=i
            ).alignment = self.alignment
            self.worksheet.cell(row=self.header_row, column=i).font = self.header_font
            self.worksheet.cell(row=self.header_row, column=i).border = self.border
            self.worksheet.cell(row=self.header_row, column=i).wrap_text = self.wrap_text
            i += 1

        i = self.header_row + 1
        while i <= self.rows:
            j = self.header_column
            while j <= self.columns:
                self.worksheet.cell(row=i, column=j).alignment = self.alignment
                self.worksheet.cell(row=i, column=j).border = self.border
                self.worksheet.cell(row=i, column=j).font = self.normal_font
                j += 1
            i += 1

        self.column_width_adjuster()
        # self.save()
