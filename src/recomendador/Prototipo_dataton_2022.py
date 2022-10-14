
#from pydoc import Helper
import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'
import re
import json

from ImpalaHelper.Impala_Helper import Helper

cache = {
            "connStr" : "DSN=impala-prod" , # Cambiar de ser necesario
            "db" : "proceso_cap_analit_y_gob_de_inf"
        }

hp = Helper(cache)

clientes = pd.read_csv("C:/Users/jucadiaz/Datos/Dataton_2022/clientes_prototipo.csv", sep = ',')
clientes.dtypes
clientes.head()


clientes["nombre"] = clientes["nombre"].apply(lambda x: re.sub("\\ssa\\b|\\sltda?\\b", "",x.lower()))
clientes["nombre"] 
clientes.shape


clie_news = pd.read_csv("C:/Users/jucadiaz/Datos/Dataton_2022/clientes_noticias_prototipo.csv", sep = ',')
clie_news.head()
clie_news.dtypes

data_news = pd.read_csv("C:/Users/jucadiaz/Datos/Dataton_2022/noticias_prototipo.csv", sep = ',')
data_news.head()
data_news.dtypes
data_news.columns

data_news[['news_title', 'news_text_content']] = data_news[['news_title', 'news_text_content']].astype(str)


with open("C:/Users/jucadiaz/Datos/Dataton_2022/diccionario_categorias.json") as file:
    dict_categoria = json.load(file)


# list_reg_exp = []
# for i in dict_categoria:
#     regular_exp = "\\b" + '\\b|\\b'.join(dict_categoria[i]) + "\\b"
#     list_reg_exp = list_reg_exp + [regular_exp]

# list_reg_exp

#i = 900378212
data_result = pd.DataFrame()

column_select = ['nit', 'new_id', 'news_url_absolute', 'news_init_date', 
    'news_final_date', 'news_title', 'count_name_tittle',
    'count_name_text', 'count_sect_tittle', 'count_sect_text','count_ciiu_tittle', 'count_ciiu_text'
       ]


for j, i in enumerate(clientes["nit"]):
    clie_news_filter = clie_news[clie_news["nit"]==i][["new_id", "nit"]]
    data_filter = pd.merge(data_news, clie_news_filter, on= "new_id")
    nombre = clientes[clientes["nit"]==i]["nombre"].iloc[0]
    sector = clientes[clientes["nit"]==i]["subsec"].iloc[0]
    ciiu = clientes[clientes["nit"]==i]["desc_ciuu_grupo"].iloc[0]
    
    data_filter["count_name_tittle"] = data_filter["news_title"].apply(lambda x: len(re.findall(nombre, x, re.IGNORECASE)))
    data_filter["count_name_text"] = data_filter["news_text_content"].apply(lambda x: len(re.findall(nombre, x, re.IGNORECASE)))
    data_filter["count_sect_tittle"] = data_filter["news_title"].apply(lambda x: len(re.findall(sector, x, re.IGNORECASE)))
    data_filter["count_sect_text"] = data_filter["news_text_content"].apply(lambda x: len(re.findall(sector, x, re.IGNORECASE)))
    data_filter["count_ciiu_tittle"] = data_filter["news_title"].apply(lambda x: len(re.findall(ciiu, x, re.IGNORECASE)))
    data_filter["count_ciiu_text"] = data_filter["news_text_content"].apply(lambda x: len(re.findall(ciiu, x, re.IGNORECASE)))
    
    data_result = pd.concat([data_result,data_filter[column_select]], ignore_index=True)
    print("Termine de contar el cliente:" + str(i) + " avance " + str((j + 1)/len(clientes["nit"])))


data_result.columns
data_result.shape

data_news2 = data_news.copy()

for i in dict_categoria:
    regular_exp = "\\b" + '(s)?(es)?\\b|\\b'.join(list(dict_categoria[i])) + "(s)?(es)?\\b"
    nom_colum1 = "count_" + i + "_tittle"
    nom_colum2 = "count_" + i + "_text"
    
    data_news2[nom_colum1] = data_news2["news_title"].apply(lambda x: len(re.findall(regular_exp, x, re.IGNORECASE)))
    data_news2[nom_colum2] = data_news2["news_text_content"].apply(lambda x: len(re.findall(regular_exp, x, re.IGNORECASE)))

    
data_news2.dtypes
data_news2.columns

columnas2 = ['new_id',
       'count_macroeconomicas_tittle', 'count_macroeconomicas_text', 'count_sostenibilidad_tittle',
       'count_sostenibilidad_text', 'count_innovacion_tittle',
       'count_innovacion_text', 'count_regulaciones_tittle',
       'count_regulaciones_text', 'count_alianzas_tittle',
       'count_alianzas_text', 'count_reputacion_tittle',
       'count_reputacion_text']

data_result2 = pd.merge(clie_news[["nit", "new_id"]], data_news2[columnas2], on = "new_id", how = "left")
data_result2.columns
data_result2.shape

data_result_final_aux = pd.merge(data_result, data_result2, on = ["nit", "new_id"])

data_result_final = pd.merge(clientes[["nit", "nombre"]], data_result_final_aux, on = "nit")


data_result_final.columns
data_result_final.dtypes
data_result_final.shape

#data_result_final.to_csv("C:/Users/jucadiaz/Datos/Dataton_2022/check_poin1.csv")

with open("C:/Users/jucadiaz/opcionesServer.json") as file:
    opt_srvr = json.load(file)


tablename = 'proceso_cap_analit_y_gob_de_inf.Dataton22_Base_conteo_prototipo'

hp.fromPandasDF(data_result_final , tablename , serverOpts = opt_srvr)



# %%
