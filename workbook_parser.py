import os
import traceback
import polars as pl
from queue import Queue
from pprint import pprint
from messages import Messagebox
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
                
                data_indices = [i for i, is_null in enumerate(null_mask) if not is_null]
                
                # 3. Group contiguous indices into blocks (sections)
                sections_indices = []
                current_section = [data_indices[0]]
                
                for i in range(1, len(data_indices)):
                    if data_indices[i] == data_indices[i-1] + 1:
                        current_section.append(data_indices[i])
                    else:
                        sections_indices.append(current_section)
                        current_section = [data_indices[i]]
                sections_indices.append(current_section)

                 # 4. Create the dictionary mapping Section Name -> Section DataFrame
                sectional_dict = {}

                for indices in sections_indices:
                    block_df = main_df.slice(indices[0], len(indices))
                    
                    # Get the section name from the first non-null element of the first row
                    first_row = block_df.row(0)
                    section_name = next((str(val) for val in first_row if val is not None), None)
                    
                    if section_name:
                        data_content = block_df.slice(1)
                        
                        # Cleaning step: Remove columns that are entirely null in this specific section
                        data_content = data_content.select([
                            col for col in data_content.get_columns() 
                            if not col.is_null().all()
                        ])
                        
                        sectional_dict[section_name] = data_content
                        
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
            
            if self.queue.qsize() > 0:
                while self.queue.qsize() > 0:
                    sheet_name, sectional_dict = self.queue.get()
                    self.dict[sheet_name] = sectional_dict
                    
        
        except Exception as e:
            messagebox = Messagebox()
            messagebox.showerror("Error", f"{traceback.format_exc()}\n{e}")
            
    
    @property
    def get_dict(self):
        return self.dict
        