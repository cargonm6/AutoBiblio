import logging
import os
import shutil
import sys
from datetime import datetime

import pandas as pd
from scidownl import scihub_download

from pdfc import compress

clean_registers = True

logger = logging.getLogger()
log_path = os.path.join(os.getcwd(), "log/").replace("\\", "/")
log_file = log_path + datetime.now().strftime("%Y-%m-%d_%H-%M-%S") + ".log"
logging.info("Log registered in \"" + log_file + "\"")
logging.basicConfig(filename=log_file, encoding='utf-8', level=logging.WARNING,
                    format="%(asctime)s [%(levelname)s] %(message)s", force=True)

res_path = os.path.join(os.getcwd(), "res/").replace("\\", "/")
doc_path = os.path.join(res_path, "docs/").replace("\\", "/")
doc_comp = os.path.join(res_path, "docs/compress/").replace("\\", "/")
csv_file = "result_query.csv"


def file_move(p_origin: str, p_destination: str, p_replace: bool = False):
    """
    Mueve/renombra un fichero.
    :param p_origin: dirección de origen.
    :param p_destination: dirección de destino.
    :param p_replace: determina si, en caso de existir el destino, este se reemplazará (falso por defecto).
    :return:
    """
    # Si no existe el fichero de destino, o se habilita el reemplazo
    if not os.path.isfile(p_destination) or p_replace:
        os.remove(p_destination) if os.path.isfile(p_destination) else 0
        os.rename(p_origin, p_destination)


def compress_docs():
    """
    Comprime los PDF de la carpeta "doc_path" en el subdirectorio "compress", y los reemplaza si su tamaño es menor.
    :return:
    """
    pdf_files = get_files(doc_path, ".pdf")

    for p_input in pdf_files:
        p_output = os.path.join(doc_comp, p_input).replace("\\", "/")
        compress(input_file_path=p_input, output_file_path=p_output, power=3)

        # Si el fichero generado es más reducido, reemplaza el original y, en caso contrario, lo elimina.
        file_move(p_output, p_input, True) if os.stat(p_output).st_size < os.stat(p_input).st_size else \
            os.remove(p_output)
        exit(0)


def get_files(p_path, p_ext):
    """
    Devuelve las rutas de todos los archivos de un directorio que cumplen cierta extensión.
    :param p_path: directorio de interés.
    :param p_ext: extensión.
    :return:
    """
    file_lst = []
    for p_file in os.listdir(p_path):
        if p_file.endswith(p_ext):
            file_lst.append(os.path.join(p_path, p_file).replace("\\", "/"))
    return file_lst


def remove_duplicates(p_data: pd.DataFrame, p_discard_col: str = None, p_subset: str = None) -> pd.DataFrame:
    """
    Elimina los elementos duplicados de un DataFrame.
    :param p_data: DataFrame.
    :param p_discard_col: columnas que no se considerarán (nulo por defecto).
    :param p_subset: selección de columnas a considerar (nulo por defecto).
    :return:
    """
    # Retira registros duplicados
    original_length = len(p_data)

    if p_subset is not None:
        p_data.drop_duplicates(subset=p_subset, inplace=True)

    elif p_discard_col is not None:
        p_data.drop_duplicates(subset=p_data.columns.difference([p_discard_col]), inplace=True)

    else:
        p_data.drop_duplicates(inplace=True)

    print("\n> Eliminados %d registros duplicados (%d -> %d)" % (
        abs(len(p_data) - original_length), original_length, len(p_data)))

    return p_data


def replace_special_characters(p_text):
    """
    Reemplaza caracteres especiales, incompatibles con el sistema de fichero Windows.
    :param p_text: texto con caracteres especiales.
    :return:
    """
    # https://www.mtu.edu/umc/services/websites/writing/characters-avoid/
    p_bad = ["#", "%", "&", "{", "}", "\\",
             "<", ">", "*", "?", "/", " ",
             "$", "!", "'", "\"", ":", "@",
             "+", "`", "|", "=", "[", "]"]

    p_good = ["_", "_", "_", "(", ")", "-",
              ".", ".", "_", "_", "-", "_",
              "_", "_", "_", "_", ".", "_",
              "_", ".", "-", "_", "(", ")"]

    for i, x in enumerate(p_bad):
        p_text = p_text.replace(x, p_good[i])

    return p_text


def get_docs(p_data):
    """
    Descarga los documentos indexados en un DataFrame, en función del campo "DOI" o "Title".
    :param p_data: Dataframe con un documento por fila.
    :return:
    """
    print("\nObtención de ficheros")
    p_data.insert(loc=1, column="DOC path", value=[None] * len(p_data))

    for i, (index, row) in enumerate(p_data.iterrows()):

        file_title = replace_special_characters(row["Title"])
        file_prefix = row["Authors"] + " " + "(" + str(row["Year"]) + ") "
        file_name = (file_prefix + file_title)[0:40] + ".pdf"

        file_path = os.path.join(doc_path, file_name).replace("\\", "/")

        proxies = {'http': 'socks5://127.0.0.1:7890'}

        # Si la entrada no tiene DOI, se descarta
        if not pd.isna(row["DOI"]):
            sys.stdout.write("\r> (%d/%d) Descargando (DOI %s)... " % (i + 1, len(p_data), row["DOI"]))
            paper_type = "doi"
            paper = row["DOI"]
            scihub_download(paper, paper_type=paper_type, out=file_path, proxies=proxies)

        elif pd.isna(row["DOI"]) or not os.path.isfile(file_path):
            sys.stdout.write(
                "\r> (%d/%d) Descargando (\"%s\")... " % (i + 1, len(p_data), row["Title"][:10] + "..."))
            paper_type = "title"
            paper = row["Title"]
            scihub_download(paper, paper_type=paper_type, out=file_path, proxies=proxies)

        if not os.path.isfile(file_path) or (os.path.isfile(file_path) and os.stat(file_path).st_size <= 200):
            os.remove(file_path) if os.path.isfile(file_path) else 0
        # Si se descargó el PDF, actualiza la entrada del DataFrame
        else:
            p_data.at[index, "DOC path"] = file_path
            p_data.to_csv(os.path.join(res_path, csv_file).replace("\\", "/"),
                          sep=";", decimal=",", encoding="utf-8", index_label="ID")
    return p_data


def generate_register(p_data, p_folder, p_ext=".csv", p_sep=None):
    """
    Genera o agrega a un DataFrame los registros de archivos de texto (txt/csv), en función de su origen.
    :param p_data: Dataframe de entrada.
    :param p_folder: Carpeta con los registros.
    :param p_ext: Extensión de los registros (por defecto, csv).
    :param p_sep: Separador (por defecto, nulo).
    :return:
    """
    # Web of Science: Obtiene todos los registros en formato TXT
    files = get_files(p_folder, p_ext)

    for idx, p_file in enumerate(files):
        sys.stdout.write("\r> (%d/%d) Leyendo fichero \"%s\"" % (idx + 1, len(files), p_file))
        file_data = pd.read_csv(p_file) if p_sep is None else pd.read_csv(p_file, sep=p_sep)
        file_data.insert(0, 'RES path', "")
        file_data["RES path"] = [p_file] * len(file_data)
        p_data = file_data if p_data is None else pd.concat([p_data, file_data], ignore_index=True)

    return p_data


def main():
    """
    Módulo principal de la aplicación.
    :return:
    """

    # Detecta las carpetas de fuentes bibliográficas
    folders = [x[0].replace("\\", "/") for x in os.walk(res_path)]
    folders = folders[1:] if len(folders) > 1 else None
    folders = [x for x in folders if "docs" not in x]

    concat_lists = []

    for folder in folders:

        # Acumula cada documento de registros en un DataFrame
        concat_data = None

        if "wos" in folder:
            print("\nLectura de referencias WoS")
            # Web of Science: Obtiene todos los registros en formato TXT
            concat_data = generate_register(p_data=concat_data, p_folder=folder, p_ext=".txt", p_sep="\t")

            concat_data = remove_duplicates(p_data=concat_data, p_discard_col="RES path")
            concat_data = concat_data[["RES path", "TI", "PY", "AU", "DI"]]
            concat_data.rename(columns={"TI": "Title", "PY": "Year", "AU": "Authors", "DI": "DOI"}, inplace=True)
            concat_data['Authors'] = concat_data['Authors'].str.split(', ').str[0]
            concat_data['Year'] = concat_data['Year'].astype('int')

        elif "scopus" in folder:
            print("\nLectura de referencias SCOPUS")
            # Web of Science: Obtiene todos los registros en formato CSV
            concat_data = generate_register(p_data=concat_data, p_folder=folder)

            concat_data = remove_duplicates(p_data=concat_data, p_discard_col="RES path")
            concat_data = concat_data[["RES path", "Title", "Year", "Authors", "DOI"]]
            concat_data['Authors'] = concat_data['Authors'].str.split(', ').str[0]
            concat_data['Authors'] = concat_data['Authors'].replace("[No author name available]", "unknown")
            concat_data['Year'] = concat_data['Year'].astype('int')

        elif "ieee" in folder:
            print("\nLectura de referencias IEEE")
            # Web of Science: Obtiene todos los registros en formato CSV
            concat_data = generate_register(p_data=concat_data, p_folder=folder)

            concat_data = remove_duplicates(p_data=concat_data, p_discard_col="RES path")
            concat_data = concat_data[["RES path", "Document Title", "Publication Year", "Authors", "DOI"]]
            concat_data.rename(columns={"Document Title": "Title", "Publication Year": "Year"}, inplace=True)
            concat_data['Authors'] = concat_data['Authors'].str.split('; ').str[0].str.split('. ').str[-1]
            concat_data['Year'] = concat_data['Year'].astype('int')

        concat_lists.append(concat_data)

    # Unifica todas las listas de referencias bibliográficas
    print("\nUnificando listas de referencias")

    concat_data = pd.concat(concat_lists, ignore_index=True)

    # Elimina títulos duplicados
    lista = [x.replace(" ", "").replace("-", "").lower() for x in concat_data["Title"]]
    concat_data["doc_title"] = lista
    concat_data = remove_duplicates(p_data=concat_data, p_subset="doc_title")
    concat_data = concat_data[["RES path", "Title", "Year", "Authors", "DOI"]].sort_values(
        by=['Year', 'Title']).reset_index(drop=True)

    # Ordena y almacena el estado actual
    concat_data.to_csv(os.path.join(res_path, csv_file).replace("\\", "/"),
                       sep=";", decimal=",", encoding="utf-8", index_label="ID")

    # Limpia los documentos generados (si está habilitado "clean_registers")
    for directory in [doc_path, doc_comp]:
        if os.path.isdir(directory):
            for file in os.listdir(directory):
                try:
                    os.remove(os.path.join(directory, file).replace("\\", "/"))
                except Exception as e:
                    print(e)
            try:
                shutil.rmtree(directory) if (os.path.isdir(directory) and clean_registers) else 0
            except Exception as e:
                print(e)
        os.mkdir(directory) if not os.path.isdir(directory) else 0

    # Descarga los documentos
    concat_data = get_docs(p_data=concat_data)

    concat_data.to_csv(os.path.join(res_path, csv_file).replace("\\", "/"),
                       sep=";", decimal=",", encoding="utf-8", index_label="ID")

    # Comprime los documentos descargados
    compress_docs()


if __name__ == '__main__':
    main()
