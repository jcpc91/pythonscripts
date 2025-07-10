import unittest
import json
import os
import pyarrow as pa
import pyarrow.parquet as pq

# Asumimos que json_to_parquet.py está en el mismo directorio o en PYTHONPATH
from json_to_parquet import json_schema_to_pyarrow_schema, make_schema_nullable, json_to_parquet

class TestJSONToParquet(unittest.TestCase):

    def setUp(self):
        # Crear archivos de esquema y datos de prueba
        self.test_schema_basic_path = "test_schema_basic_for_tests.json"
        self.test_schema_nested_path = "test_schema_nested_for_tests.json"
        self.test_data_path = "test_data_for_tests.json"
        self.output_parquet_path = "test_output.parquet"
        self.output_pyarrow_schema_path = "pyarrow_schema_from_json_schema.json" # Nombre de archivo por defecto

        with open(self.test_schema_basic_path, 'w') as f:
            json.dump({
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "age": {"type": "integer"},
                    "salary": {"type": "number"},
                    "isStudent": {"type": "boolean"},
                    "mixedType": {"type": ["integer", "null"]}
                }
            }, f)

        with open(self.test_schema_nested_path, 'w') as f:
            json.dump({
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "string"},
                        "person": {
                            "type": "object",
                            "properties": {
                                "firstName": {"type": "string"},
                                "lastName": {"type": "string"}
                            }
                        },
                        "tags": {"type": "array", "items": {"type": "string"}},
                        "scores": {"type": "array", "items": {"type": "integer"}},
                        "address": {
                            "type": "object",
                            "properties": {"street": {"type": "string"}, "city": {"type": "string"}}
                        },
                        "nullableString": {"type": ["string", "null"]},
                        "alwaysNull": {"type": "null"}
                    }
                }
            }, f)

        with open(self.test_data_path, 'w') as f:
            json.dump([
                {
                    "id": "1",
                    "person": {"firstName": "John", "lastName": "Doe"},
                    "tags": ["alpha", "beta"],
                    "scores": [10, 20],
                    "address": {"street": "123 Main St", "city": "Anytown"},
                    "nullableString": "value1",
                    "alwaysNull": None
                },
                {
                    "id": "2",
                    "person": {"firstName": "Jane", "lastName": "Doe"},
                    "tags": ["gamma"],
                    "scores": [30, 40, 50],
                    "address": {"street": "456 Oak St", "city": "Otherville"},
                    "nullableString": None,
                    "alwaysNull": None
                }
            ], f)

    def tearDown(self):
        # Limpiar archivos creados
        for path in [
            self.test_schema_basic_path, self.test_schema_nested_path,
            self.test_data_path, self.output_parquet_path,
            self.output_pyarrow_schema_path # Limpiar también el schema guardado
        ]:
            if os.path.exists(path):
                os.remove(path)

    def test_json_schema_to_pyarrow_schema_basic(self):
        with open(self.test_schema_basic_path, 'r') as f:
            schema_json = json.load(f)

        pyarrow_schema = json_schema_to_pyarrow_schema(schema_json)

        expected_fields = {
            "name": pa.string(),
            "age": pa.int64(),
            "salary": pa.float64(),
            "isStudent": pa.bool_(),
            "mixedType": pa.int64() # Toma el primer tipo no nulo
        }

        self.assertEqual(len(pyarrow_schema.names), len(expected_fields))
        for name, pa_type in expected_fields.items():
            self.assertEqual(pyarrow_schema.field(name).type, pa_type)
            # Inicialmente, los campos no son anulables por json_schema_to_pyarrow_schema
            self.assertFalse(pyarrow_schema.field(name).nullable)


    def test_json_schema_to_pyarrow_schema_nested_and_array(self):
        with open(self.test_schema_nested_path, 'r') as f:
            schema_json = json.load(f) # Es un array, la función maneja 'items'

        pyarrow_schema = json_schema_to_pyarrow_schema(schema_json)

        # Verificar campos de nivel superior
        self.assertEqual(pyarrow_schema.field("id").type, pa.string())
        self.assertTrue(pa.types.is_struct(pyarrow_schema.field("person").type))
        self.assertTrue(pa.types.is_list(pyarrow_schema.field("tags").type))
        self.assertTrue(pa.types.is_list(pyarrow_schema.field("scores").type))
        self.assertTrue(pa.types.is_struct(pyarrow_schema.field("address").type))
        self.assertEqual(pyarrow_schema.field("nullableString").type, pa.string()) # Toma string de ["string", "null"]
        self.assertEqual(pyarrow_schema.field("alwaysNull").type, pa.null())


        # Verificar estructura anidada de 'person'
        person_struct = pyarrow_schema.field("person").type
        self.assertEqual(person_struct.field("firstName").type, pa.string())
        self.assertEqual(person_struct.field("lastName").type, pa.string())

        # Verificar tipo de item de 'tags'
        tags_list_type = pyarrow_schema.field("tags").type
        self.assertEqual(tags_list_type.value_type, pa.string()) # value_type para pa.list_

        # Verificar tipo de item de 'scores'
        scores_list_type = pyarrow_schema.field("scores").type
        self.assertEqual(scores_list_type.value_type, pa.int64())


    def test_make_schema_nullable(self):
        with open(self.test_schema_nested_path, 'r') as f:
            schema_json = json.load(f)
        base_schema = json_schema_to_pyarrow_schema(schema_json)
        nullable_schema = make_schema_nullable(base_schema)

        # Todos los campos de nivel superior deben ser anulables
        for field_name in nullable_schema.names:
            self.assertTrue(nullable_schema.field(field_name).nullable, f"Field {field_name} should be nullable")

        # Verificar campos anidados en 'person' (struct)
        person_struct_type = nullable_schema.field("person").type
        self.assertTrue(person_struct_type.field("firstName").nullable)
        self.assertTrue(person_struct_type.field("lastName").nullable)

        # Verificar que el campo del valor de la lista 'tags' sea anulable
        # make_schema_nullable ahora hace que el pa.list_(field) tenga el field.nullable = True
        # y el campo que representa el tipo de valor de la lista también será anulable si es complejo.
        tags_list_field = nullable_schema.field("tags")
        self.assertTrue(tags_list_field.nullable) # El campo de lista en sí es anulable
        self.assertTrue(pa.types.is_list(tags_list_field.type))
        # El value_field de un pa.list_ también debe ser nullable
        self.assertTrue(tags_list_field.type.value_field.nullable, "List value field for 'tags' should be nullable")


        # Verificar 'address' (struct)
        address_struct_type = nullable_schema.field("address").type
        self.assertTrue(address_struct_type.field("street").nullable)
        self.assertTrue(address_struct_type.field("city").nullable)

        # Verificar 'alwaysNull' que es pa.null()
        always_null_field = nullable_schema.field("alwaysNull")
        self.assertTrue(always_null_field.nullable)
        self.assertTrue(pa.types.is_null(always_null_field.type))


    def test_full_json_to_parquet_conversion(self):
        # Ejecutar la conversión principal
        json_to_parquet(self.test_data_path, self.output_parquet_path, self.test_schema_nested_path, batch_size=1)

        # Verificar que el archivo Parquet fue creado
        self.assertTrue(os.path.exists(self.output_parquet_path))

        # Leer el archivo Parquet y verificar su esquema y datos
        parquet_table = pq.read_table(self.output_parquet_path)
        parquet_schema = parquet_table.schema

        # Cargar el esquema JSON original y convertirlo a PyArrow + nullable para comparación
        with open(self.test_schema_nested_path, 'r') as f:
            original_json_schema_content = json.load(f)
        expected_base_schema = json_schema_to_pyarrow_schema(original_json_schema_content)
        expected_final_schema = make_schema_nullable(expected_base_schema)

        # Comparar campos uno por uno porque los metadatos pueden diferir ligeramente
        self.assertEqual(len(parquet_schema.names), len(expected_final_schema.names))
        for name in expected_final_schema.names:
            self.assertTrue(name in parquet_schema.names, f"Field {name} missing in Parquet schema")
            expected_field = expected_final_schema.field(name)
            actual_field = parquet_schema.field(name)
            self.assertEqual(actual_field.type, expected_field.type, f"Type mismatch for field {name}: Parquet={actual_field.type}, Expected={expected_field.type}")
            self.assertEqual(actual_field.nullable, expected_field.nullable, f"Nullability mismatch for field {name}")

        # Verificar el número de filas
        self.assertEqual(len(parquet_table), 2) # Basado en test_data.json

        # Verificar algunos datos (opcional, pero bueno para la confianza)
        data_as_pydict = parquet_table.to_pydict()
        self.assertEqual(data_as_pydict['id'], ['1', '2'])
        self.assertEqual(data_as_pydict['person'][0]['firstName'], 'John')
        self.assertEqual(data_as_pydict['person'][1]['lastName'], 'Doe')
        self.assertListEqual(data_as_pydict['tags'][0], ['alpha', 'beta'])
        self.assertIsNone(data_as_pydict['nullableString'][1])
        self.assertIsNone(data_as_pydict['alwaysNull'][0]) # Todos los valores de un campo pa.null() son None

        # Verificar que el archivo de esquema de pyarrow se haya guardado
        self.assertTrue(os.path.exists(self.output_pyarrow_schema_path))
        with open(self.output_pyarrow_schema_path, 'r') as f:
            saved_pyarrow_schema_json = json.load(f)
        self.assertEqual(len(saved_pyarrow_schema_json['fields']), len(expected_final_schema.names))


if __name__ == '__main__':
    # Esto permite ejecutar las pruebas con `python test_json_to_parquet.py`
    # y también con `python -m unittest test_json_to_parquet.py`
    unittest.main(argv=['first-arg-is-ignored'], exit=False)
