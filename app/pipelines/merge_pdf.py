import PyPDF2
import os

merger = PyPDF2.PdfMerger()
input_dir = r"c:\Users\RafaelSouza\VERUM PARTNERS\VERUM PARTNERS - VAL2018021\00.TI\PDFs merge"
output_dir = r"c:\Users\RafaelSouza\VERUM PARTNERS\VERUM PARTNERS - VAL2018021\00.TI\PDFs merge\output"
environ_os = os.environ
# print(environ_os)
lista_arquivos = os.listdir(input_dir)
for arquivo in lista_arquivos:
    if ".pdf" in arquivo:
        print(arquivo)
        merger.append(fr"c:\Users\RafaelSouza\VERUM PARTNERS\VERUM PARTNERS - VAL2018021\00.TI\PDFs merge/{arquivo}")

merger.write(output_dir+"\PDF_Final.PDF")




