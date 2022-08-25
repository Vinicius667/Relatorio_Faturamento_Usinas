import pandas as pd 
import os 
import numpy as np

# Junta arquivos em um dataframe
def concat_rrvs(file_paths,colunas):
    df = pd.DataFrame(columns=colunas)
    parquet_criado = False
    for file_path in file_paths:
        file_name = file_path.split("\\")[1].split(".")[0]
        parquet_file_path= f"./Inputs/RRVs/Parquets/{file_name}.gzip"
        if os.path.exists(parquet_file_path):
            print(f"Lendo arquivo: {parquet_file_path}")
            df_dummy = pd.read_parquet(parquet_file_path)
            parquet_criado = True
        else: 
            print(f"Lendo arquivo: {file_path}")
            df_dummy = pd.read_excel(file_path)[colunas]
            
            
        meses_repetidos = np.intersect1d(df["Mês/Ano"].unique(), df_dummy["Mês/Ano"].unique())
        if len(meses_repetidos) > 0:
            raise ValueError(f"No relatório {file_path} há meses que também estão em outro relatório: {', '.join(meses_repetidos)}.")
        if not parquet_criado:
            print(f"Criando arquivo: {parquet_file_path}")
            df_dummy.to_parquet(parquet_file_path)
        df = pd.concat([df,df_dummy],ignore_index=True)
    return df
