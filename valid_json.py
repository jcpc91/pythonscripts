import json
import sys
from jsonschema import validate, ValidationError

def validar_json(archivo_json, archivo_schema):
    with open(archivo_schema, 'r', encoding='utf-8') as f:
        schema = json.load(f)
    with open(archivo_json, 'r', encoding='utf-8') as f:
        datos = json.load(f)
    try:
        validate(instance=datos, schema=schema)
        print("El archivo JSON es válido según el schema.")
    except ValidationError as e:
        print("El archivo JSON NO es válido:")
        print(e)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Uso: python valid_json.py datos.json schema.json")
        sys.exit(1)
    archivo_json = sys.argv[1]
    archivo_schema = sys.argv[2]
    validar_json(archivo_json, archivo_schema)