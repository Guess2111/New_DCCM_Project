import os
import traceback
import polars as pl
import polars.selectors as cs
from queue import Queue
from pprint import pprint
from messages import Messagebox
from tabulate import tabulate
from threading import Event
from concurrent.futures import ThreadPoolExecutor



class Excel_Reader_and_Template_Maker:
    def __init__(self, file_path: str):
        try:
            if os.path.exists(str(file_path)):
                self.file_path = str(file_path)
                self.total_dataframe_dict = {}
                self.dict = {}
                self.queue = Queue()
            
            self.workbook = pl.read_excel(self.file_path, sheet_id = 0)
            self.sheets = list(self.workbook.keys())
            
            if not self.sheets:
                raise Exception("Sheets Not Found!")
                
        except FileNotFoundError:
            messagebox = Messagebox()
            messagebox.showerror("File Not Found", "File Not Found!")
        
        except Exception as e:
            messagebox = Messagebox()
            messagebox.showerror("Error", f"{traceback.format_exc()}\n{e}")
    
    
    def sheet_parser(self, sheet_name: str, main_df: pl.DataFrame):
        stop_event = Event()
        try:
            if main_df.shape[0] > 0:
                null_mask = main_df.select(
                    (
                        pl.all().is_null()
                    ) 
                    | 
                    (
                        pl.all().cast(pl.Utf8) == ""
                    )
                ).to_series()
                
                # print(f"\n\n{sheet_name = }, {null_mask = }\n\n")
                # print(f"\n\n{sheet_name = }, {main_df[7, :] = }\n\n")
                # print(f"\n\n{sheet_name = }, {main_df[8, :] = }\n\n")
                # print(f"\n\n{sheet_name = }, {main_df[9, :] = }\n\n")
                
                data_indices = [i for i, is_null in enumerate(null_mask) if not is_null]
                
                data_indices.append(main_df.shape[0]+1)
                # print(f"\n\n{sheet_name = }\n{data_indices = }\n\n") 
                
                sectional_block_tuple_list = []
                i = 0
                while i+1 < len(data_indices):
                    temp_tuple = ()
                    if len(sectional_block_tuple_list) == 0:
                        temp_tuple = (0, data_indices[i])

                    elif i+1 < len(data_indices) and  data_indices[i]+1 == data_indices[i+1]:
                            i += 1
                    
                    else:
                        temp_tuple = (data_indices[i], data_indices[i+1])
                        i += 1
                    
                    if temp_tuple:
                        sectional_block_tuple_list.append(temp_tuple)
                
                main_df_columns = main_df.columns
                # print(f"\n\n{sheet_name = }\n\n{main_df_columns = }")
                # print(f"\n\n{sheet_name = }\n{sectional_block_tuple_list = }\n\n")
                sectional_dict = {}    
                i = 0
                while i < len(sectional_block_tuple_list):
                    first_row, last_row = sectional_block_tuple_list[i]
                    start_row = 0
                    # print(f"\n\n{sheet_name = }\n{i = }\n{first_row = }\n{last_row = }\n")
                    temp_df = pl.DataFrame()
                    section_name = ""
                    exclude_columns = [0]
                    first_row_for_column = ()
                    
                    if len(sectional_dict) == 0:
                        # print(f"{sheet_name = }, {first_row = }")
                        columns = main_df.columns
                        first_row_for_column = main_df.columns
                        start_row = first_row
                        columns = columns[1:]
                        
                    
                    else:
                        # print(f"\n\n{sheet_name = },\n{first_row = }\n{[str(value).strip() for _, value in main_df.row(first_row, named =True).items() if value is not None] = }\n\n")
                        first_row_for_column = [str(value).strip() for _, value in main_df.row(first_row, named =True).items() if value is not None]
                        # print(f"{sheet_name = }, {first_row_for_column = }")
                        start_row = first_row + 1
                        
                    
                    columns = [element for element in first_row_for_column if element is not None or element != ""]
                    # print(f"{sheet_name = }\n{i = }\n{columns = }")
                    starting_column_index_to_remove = len(columns)
                    ending_column_index_to_remove = len(main_df_columns)
                    exclude_columns.extend(
                        [i for i in range(starting_column_index_to_remove, ending_column_index_to_remove)]
                    )
                    
                    section_name = str(columns[0]).strip()
                    # columns = [str(columns[i].value).strip() for i in range(1, len(columns))]
                    # print(f"{sheet_name=}\n{columns=}")
                    temp_df = main_df[start_row:last_row, :]
                    # print(f"\n\n{sheet_name = }\n{i = }\n{temp_df}\n")                    
                    
                    if len(exclude_columns) > 0:
                        # print(f"\n\n{sheet_name = }\n{i = }\n{exclude_columns}\n")
                        # print(f"\n\n{sheet_name = }\n{i = }\n{cs.by_index(
                        #             list(set(exclude_columns))
                        #         ) = }\n")
                        temp_df = temp_df.select(~cs.by_index(
                                    list(set(exclude_columns))
                                ))
                        # print(f"\n\n{sheet_name = }\n{i = }\n{temp_df}\n")
                    # print(f"\n\n{sheet_name = }\n{i = }\n{columns=}")
                    temp_df = temp_df.rename(
                        dict(
                            zip(
                                [element for element in temp_df.columns],
                                columns[1:]
                            )
                        )
                    )
                    # print(f"\n\n{sheet_name = }\n{i = }\n{temp_df}\n")
                    
                    sectional_dict[section_name] = temp_df
                    
                    i += 1        
                # print(f"\n\n{sheet_name = }\n {sectional_dict}")
                self.queue.put((sheet_name, sectional_dict))
                
        except Exception as e:
            messagebox = Messagebox()
            messagebox.showerror("Error", f"{traceback.format_exc()}\n{e}")
        
        
        if not stop_event.is_set():
            stop_event.set()    
    
    
    def file_parser(self):
        try:
            # print(self.sheets)
            # print(
            #     "\n\n".join(f'{sheet}:\n{df}' for sheet, df in self.workbook.items())
            # )
            if self.sheets:
                with ThreadPoolExecutor(max_workers=4) as executor:
                    futures = [
                        executor.submit(
                            self.sheet_parser,
                            self.sheets[i],
                            self.workbook[self.sheets[i]]
                        )
                        for i in range(len(self.sheets))
                    ]
                    
                    for future in futures:
                        future.result()
            # print("Queue size:", self.queue.qsize())
            if self.queue.qsize() > 0:
                while self.queue.qsize() > 0:
                    print(self.queue.qsize())
                    sheet_name, sectional_dict = self.queue.get()
                    self.dict[sheet_name] = sectional_dict
                    
        
        except Exception as e:
            messagebox = Messagebox()
            messagebox.showerror("Error", f"{traceback.format_exc()}\n{e}")
    
    
    def markdown_maker(self, df: pl.DataFrame):
        try:
            markdown_table = tabulate(df.to_dicts(), headers="keys", tablefmt="github")
            return markdown_table
            
        except Exception as e:
            messagebox = Messagebox()
            messagebox.showerror("Error", f"{traceback.format_exc()}\n{e}")
    
    def writer(self):
        try:
            string_ = f"{'\n\n'.join(f'{sheet_name}:\n\t{section_name}:\n{self.markdown_maker(dataframe)}' for sheet_name, dict_ in self.dict.items() for section_name, dataframe in dict_.items())}"
            with open("test_text.txt", "w+") as f:
                f.write(string_)
                f.close()
                del f
        except Exception as e:
            messagebox = Messagebox()
            messagebox.showerror("Error", f"{traceback.format_exc()}\n{e}")
            
    
    @property
    def get_dict(self):
        return self.dict
        