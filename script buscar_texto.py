import os
import argparse

def search_in_directory(directory, text_to_search):
    """
    Busca un texto en todos los archivos de un directorio e imprime los resultados.

    Args:
        directory (str): La ruta al directorio donde buscar.
        text_to_search (str): El texto a buscar.
    """
    print("Dir|filename|lineNo")
    for dirpath, _, filenames in os.walk(directory):
        for filename in filenames:
            file_path = os.path.join(dirpath, filename)
            try:
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as file:
                    for line_number, line in enumerate(file, 1):
                        if text_to_search in line:
                            print(f"{dirpath}|{filename}|{line_number}")
            except Exception as e:
                # Ignorar errores al leer archivos (ej. permisos, etc.)
                pass

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Busca un texto en archivos de un directorio.")
    parser.add_argument("directory", help="El directorio en el que buscar.")
    parser.add_argument("text", help="El texto a buscar.")
    
    args = parser.parse_args()
    
    if not os.path.isdir(args.directory):
        print(f"Error: El directorio '{args.directory}' no existe.")
    else:
        search_in_directory(args.directory, args.text)
