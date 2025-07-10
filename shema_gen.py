import json
import sys
from genson import SchemaBuilder

def generar_schema(archivo_json, archivo_schema):
    with open(archivo_json, 'r', encoding='utf-8') as f:
        datos = json.load(f)
    builder = SchemaBuilder()
    builder.add_object(datos)
    schema = builder.to_schema()
    with open(archivo_schema, 'w', encoding='utf-8') as f:
        json.dump(schema, f, indent=2, ensure_ascii=False)
    print(f"Schema generado y guardado en {archivo_schema}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Uso: python shema_gen.py datos.json schema.json")
        sys.exit(1)
    archivo_json = sys.argv[1]
    archivo_schema = sys.argv[2]
    generar_schema(archivo_json, archivo_schema)