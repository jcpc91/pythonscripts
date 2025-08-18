import base64
import os

def convertir_archivo_a_base64(ruta_archivo: str) -> str | None:
    """
    Lee un archivo desde la ruta especificada, lo convierte a formato base64
    y retorna el resultado como una cadena de texto.

    Args:
        ruta_archivo: La ruta completa al archivo que se va a convertir.

    Returns:
        Una cadena de texto con el contenido del archivo en base64, o None
        si el archivo no se encuentra o hay un error al leerlo.
    """
    try:
        with open(ruta_archivo, "rb") as archivo:
            bytes_archivo = archivo.read()
            bytes_base64 = base64.b64encode(bytes_archivo)
            return bytes_base64.decode('utf-8')
    except FileNotFoundError:
        print(f"Error: El archivo no se encontró en la ruta '{ruta_archivo}'")
        return None
    except Exception as e:
        print(f"Ocurrió un error al leer el archivo: {e}")
        return None

def detectar_tipo_archivo(datos_base64: str) -> str:
    """
    Detecta el tipo de archivo (JPG, PNG, PDF) desde una cadena en formato base64
    utilizando los 'magic numbers' (firmas de archivo).

    Args:
        datos_base64: Una cadena de texto que representa un archivo en formato base64.

    Returns:
        Una cadena de texto que indica el tipo de archivo ("JPG", "PNG", "PDF") o
        "Tipo de archivo desconocido" si no se puede identificar, o un mensaje
        de error si la cadena base64 es inválida.
    """
    try:
        # Decodificar la cadena base64 para obtener los bytes del archivo.
        # Se necesita agregar un padding si la longitud no es múltiplo de 4.
        padding_necesario = len(datos_base64) % 4
        if padding_necesario:
            datos_base64 += '=' * (4 - padding_necesario)
        
        bytes_archivo = base64.b64decode(datos_base64)
    except (base64.binascii.Error, ValueError):
        return "Error: La cadena base64 proporcionada no es válida."

    # Comprobar que tenemos suficientes bytes para leer la firma.
    if len(bytes_archivo) < 8:
        return "Tipo de archivo desconocido (datos insuficientes)."

    # --- Definición de Magic Numbers / Firmas de Archivo ---
    # PNG: 89 50 4E 47 0D 0A 1A 0A  (en bytes: \x89PNG\r\n\x1a\n)
    firma_png = b'\x89PNG\r\n\x1a\n'
    # JPEG: FF D8 FF
    firma_jpeg = b'\xff\xd8\xff'
    # PDF: 25 50 44 46 (en bytes: %PDF)
    firma_pdf = b'%PDF'
    firma_riff = b'RIFF'
    firma_webp = b'WEBP'

    # --- Comparación ---
    if bytes_archivo.startswith(firma_png):
        return "PNG"
    elif bytes_archivo.startswith(firma_jpeg):
        return "JPG"
    elif bytes_archivo.startswith(firma_pdf):
        return "PDF"
    elif (
        bytes_archivo.startswith(firma_riff) and
        bytes_archivo[8:12] == firma_webp
    ):
        return "WEBP"
    else:
        return "Tipo de archivo desconocido"

# --- Ejemplos de Uso ---

print("--- Pruebas con cadenas Base64 predefinidas ---")
# 1. Cadena Base64 de un archivo PNG (muy simple, 1x1 pixel transparente)
#base64_png = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII="
#print(f"Archivo 1 (PNG predefinido): {detectar_tipo_archivo(base64_png)}")

# 2. Cadena Base64 de un archivo JPG (muy simple, 1x1 pixel negro)
#base64_jpg = "/9j/4AAQSkZJRgABAQEAYABgAAD/2wBDAAIBAQIBAQICAgICAgICAwUDAwMDAwYEBAMFBwYHBwcGBwcICQsJCAgKCAcHCg0KCgsMDAwMBwkODw0MDgsMDAz/wAARCAABAAEDASIAAhEBAxEB/8QAHwAAAQUBAQEBAQEAAAAAAAAAAAECAwQFBgcICQoL/8QAtRAAAgEDAwIEAwUFBAQAAAF9AQIDAAQRBRIhMUEGE1FhByJxFDKBkaEII0KxwRVS0fAkM2JyggkKFhcYGRolJicoKSo0NTY3ODk6Q0RFRkdISUpTVFVWV1hZWmNkZWZnaGlqc3R1dnd4eXqDhIWGh4iJipKTlJWWl5iZmqKjpKWmp6ipqrKztLW2t7i5usLDxMXGx8jJytLT1NXW19jZ2uHi4+Tl5ufo6erx8vP09fb3+Pn6/8QAHwEAAwEBAQEBAQEBAQAAAAAAAAECAwQFBgcICQoL/8QAtREAAgECBAQDBAcFBAQAAQJ3AAECAxEEBSExBhJBUQdhcRMiMoEIFEKRobHBCSMzUvAVYnLRChYkNOEl8RcYGRomJygpKjU2Nzg5OkNERUZHSElKU1VZXWFlaY2RlZmdoaWpzdHV2d3h5eoKDhIWGh4iJipKTlJWWl5iZmqKjpKWmp6ipqrKztLW2t7i5usLDxMXGx8jJytLT1NXW19jZ2uLj5OXm5+jp6vLz9PX29/j5+v/aAAwDAQACEQMRAD8A/v4ooooA//Z"
#print(f"Archivo 2 (JPG predefinido): {detectar_tipo_archivo(base64_jpg)}")

# 3. Cadena Base64 de un archivo PDF (muy simple, documento vacío)
#base64_pdf = "JVBERi0xLjQKJdPr6eEKMSAwIG9iago8PC9UeXBlIC9DYXRhbG9nL1BhZ2VzIDIgMCBSL0xhbmcgKGVuLVVDKSAvU3RydWN0VHJlZVJvb3QgMyAwIFIvTWFya0luZm8gPDwvTWFya2VkIHRydWU+Pj4+CmVuZG9iago"
#print(f"Archivo 3 (PDF predefinido): {detectar_tipo_archivo(base64_pdf)}")

print("\n--- Prueba convirtiendo un archivo local ---")
# **IMPORTANTE**: Cambia el valor de esta variable a la ruta de un archivo real en tu sistema.
# Por ejemplo: "C:/Users/TuUsuario/Documentos/mi_imagen.jpg" en Windows
# o "/home/tu_usuario/imagenes/foto.png" en Linux/Mac.
ruta_del_archivo = "C:/Users/jcpena/Pictures/5.webp"

if os.path.exists(ruta_del_archivo):
    # Convertir el archivo a base64
    mi_base64 = convertir_archivo_a_base64(ruta_del_archivo)

    # Si la conversión fue exitosa, detectar el tipo
    if mi_base64:
        tipo_detectado = detectar_tipo_archivo(mi_base64)
        print(f"El archivo en '{ruta_del_archivo}' es de tipo: {tipo_detectado}")
else:
    print(f"El archivo de prueba '{ruta_del_archivo}' no existe. Por favor, edita el script con una ruta válida.")

