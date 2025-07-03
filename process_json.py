import bigjson
import json

def extract_results(input_filepath, output_filepath):
    """
    Reads a large JSON file, extracts the content of the 'results' array
    under 'd', and saves it to a new JSON file.

    Args:
        input_filepath (str): Path to the input JSON file.
        output_filepath (str): Path to save the extracted results.
    """
import bigjson
import json
# Import specific types now that they are known
from bigjson.array import Array as BigJSONArray
from bigjson.obj import Object as BigJSONObject

# (Original convert_bigjson_to_native and extract_results functions remain largely the same,
# but use the imported types directly and remove discovery logic)

def convert_bigjson_to_native(bj_obj):
    """
    Recursively converts bigjson array and object instances to
    Python lists and dictionaries.
    """
    if isinstance(bj_obj, BigJSONArray):
        return [convert_bigjson_to_native(item) for item in bj_obj]
    elif isinstance(bj_obj, BigJSONObject):
        return {key: convert_bigjson_to_native(value) for key, value in bj_obj.items()}
    else:
        # For basic types (str, int, float, bool, None), return as is.
        return bj_obj

def extract_results(input_filepath, output_filepath):
    """
    Reads a large JSON file, extracts the content of the 'results' array
    under 'd', and saves it to a new JSON file.

    Args:
        input_filepath (str): Path to the input JSON file.
        output_filepath (str): Path to save the extracted results.
    """
    with open(input_filepath, 'rb') as f:
        data = bigjson.load(f)
        # Access the target array
        bigjson_results_array = data['d']['results']
        # Convert the bigjson array (and its contents) to native Python objects
        native_results_list = convert_bigjson_to_native(bigjson_results_array)

    with open(output_filepath, 'w') as outfile:
        json.dump(native_results_list, outfile, indent=4)
    print(f"Successfully extracted results from '{input_filepath}' to '{output_filepath}'")

if __name__ == "__main__":
    input_file = "bp05_1.json"
    output_file = "results.json"
    extract_results(input_file, output_file)
