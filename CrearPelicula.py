import boto3
import uuid
import os
import json

def lambda_handler(event, context):
    try:
        # Entrada (json)
        log_info = {
            "tipo": "INFO",
            "log_datos": {
                "mensaje": "Iniciando procesamiento",
                "event": event
            }
        }
        print(json.dumps(log_info))
        
        tenant_id = event['body']['tenant_id']
        pelicula_datos = event['body']['pelicula_datos']
        nombre_tabla = os.environ["TABLE_NAME"]
        
        # Proceso
        uuidv4 = str(uuid.uuid4())
        pelicula = {
            'tenant_id': tenant_id,
            'uuid': uuidv4,
            'pelicula_datos': pelicula_datos
        }
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(nombre_tabla)
        response = table.put_item(Item=pelicula)
        
        # Salida (json)
        log_info = {
            "tipo": "INFO",
            "log_datos": {
                "mensaje": "Pelicula creada exitosamente",
                "pelicula": pelicula
            }
        }
        print(json.dumps(log_info))
        
        return {
            'statusCode': 200,
            'pelicula': pelicula,
            'response': response
        }
    
    except Exception as e:
        # Manejo de errores
        log_error = {
            "tipo": "ERROR",
            "log_datos": {
                "mensaje": "Error al procesar la solicitud",
                "error": str(e),
                "tipo_error": type(e).__name__,
                "event": event
            }
        }
        print(json.dumps(log_error))
        
        return {
            'statusCode': 500,
            'error': str(e)
        }
