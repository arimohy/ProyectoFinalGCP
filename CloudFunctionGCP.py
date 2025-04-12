import functions_framework 
from google.cloud import storage
import pandas as pd
from io import StringIO

@functions_framework.http
def upload_multiple_csvs(request):
    """Función HTTP para leer múltiples archivos CSV desde un bucket y guardarlos en otro"""

    print('Ejecutando función...')

    # Buckets
    source_bucket_name = 'raw_pf'
    destination_bucket_name = 'staging_pf'

    # Archivos a procesar
    archivos = {
        'ventas.csv': 'ventas_stg.csv',
        'productos.csv': 'productos_stg.csv',
        'clientes.csv': 'clientes.csv'
    }

    # Inicializa el cliente de Cloud Storage
    storage_client = storage.Client()
    source_bucket = storage_client.bucket(source_bucket_name)
    destination_bucket = storage_client.bucket(destination_bucket_name)

    resultados = []

    for archivo_origen, archivo_destino in archivos.items():
        try:
            print(f"Procesando archivo: {archivo_origen}")
            source_blob = source_bucket.blob(archivo_origen)

            # Descargar contenido
            csv_data = source_blob.download_as_text()
            data = StringIO(csv_data)
            df = pd.read_csv(data)

            # Aquí puedes realizar transformaciones personalizadas si deseas
            # df = transformacion(df)

            # Subir archivo procesado al bucket destino
            csv_output = df.to_csv(index=False)
            destination_blob = destination_bucket.blob(archivo_destino)
            destination_blob.upload_from_string(csv_output, content_type='text/csv')

            resultados.append(f"✅ {archivo_origen} procesado como {archivo_destino}")

        except Exception as e:
            error_msg = f"x Error al procesar {archivo_origen}: {e}"
            print(error_msg)
            resultados.append(error_msg)

    # Retornar resumen de los archivos procesados
    return "\n".join(resultados), 200
