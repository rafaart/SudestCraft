import os




def filter_dir(dir_path, main_root, ignore, depth):
    return dir_path[len(main_root):].count(os.sep) > depth and all(term not in dir_path for term in ignore)

def filter_files(file_name, extensions):
    return any(ext in file_name for ext in extensions)


main_root = r'C:\Users\emman\VERUM PARTNERS\VERUM PARTNERS - VAL2018021\00.TI\Proj - Capanema\SMAT\LX'
extensions=['.xls', '.xlsx']
ignore=['.SUPERADOS', 'old', '_ETL', 'ZFORNECEDOR', 'AVANCO']
depth=2

mapped_dirs = [(root, files) for root, _, files in os.walk(main_root) if filter_dir(root, main_root, ignore, depth)]
mapped_files = [os.path.join(dir_path, file_name) for dir_path, files in mapped_dirs for file_name in files if filter_files(file_name, extensions)]
for file in mapped_files:
    print(file)
# print(mapped_files)
# for file_name in files:
#                 if any(ext.lower() in file_name[-len(ext):] for ext in extensions):
#                     print(root)
#                     print(file_name)
#                     print('\n')



            
