import os

root = r'C:\Users\emman\VERUM PARTNERS\VERUM PARTNERS - VAL2018021\00.TI\Proj - New Steel\BI\02. Repositório de Arquivos\LX\LX Geral'
files = os.listdir(root)
for file in files:
    if '_' in file:
        ext = file.split('.')[-1]
        new_name = file.split('_')[0] + '.' + ext

        old_path = os.path.join(root, file)
        new_path = os.path.join(root, new_name)

        if new_name in files:
            os.remove(new_path)
            print(new_name)
        os.rename(old_path, new_path)
        
    