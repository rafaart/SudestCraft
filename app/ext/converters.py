from pathlib import Path
import win32com.client as win32
    
def convert_xls_to_xlsx(file_paths):
    for path in file_paths:
        if '.xls' in path[-4:]:
            _converter(Path(path))

def _converter(path: Path) -> None:
    excel = win32.gencache.EnsureDispatch('Excel.Application')
    wb = excel.Workbooks.Open(path.absolute())
    
    # FileFormat=51 is for .xlsx extension
    wb.SaveAs(str(path.absolute().with_suffix(".xlsx")), FileFormat=51)
    wb.Close()
    excel.Application.Quit()
