
import os
import requests
import time
import psycopg2
import json
from urllib.parse import urlparse

# --- 1. CONFIGURACIÓN ---
# Obtener las variables de entorno configuradas en Vercel
DOMUS_TOKEN = os.environ.get('DOMUS_TOKEN')
SUPABASE_URL = os.environ.get('SUPABASE_URL')
# Parámetros de la API de Domus
DOMUS_API_BASE = "https://apiv3get.domus.la/inmuebles/lista"
LIMIT_PER_PAGE = 50
MAX_RETRIES = 5 # Número máximo de intentos si falla la conexión
RETRY_DELAY = 5 # Retraso inicial en segundos antes de reintentar

# Función principal que Vercel ejecutará
def handler(request):
    """
    Función sin servidor que consume la API de Domus por páginas 
    y guarda los datos en Supabase.
    """
    if not DOMUS_TOKEN or not SUPABASE_URL:
        return {
            'statusCode': 500,
            'body': 'Error: DOMUS_TOKEN o SUPABASE_URL no configuradas.'
        }
    
    # 1.1. Parsear la URL de Supabase para obtener las credenciales
    # Esto permite conectar usando psycopg2
    try:
        result = urlparse(SUPABASE_URL)
        db_params = {
            "database": result.path[1:],
            "user": result.username,
            "password": result.password,
            "host": result.hostname,
            "port": result.port
        }
    except Exception as e:
        return {'statusCode': 500, 'body': f"Error al parsear URL de Supabase: {e}"}

    # --- 2. LOGICA DE EXTRACCIÓN Y PAGINACIÓN ---
    page = 1
    total_inmuebles_actualizados = 0
    inmuebles_en_pagina = LIMIT_PER_PAGE # Inicializar para entrar en el bucle

    while inmuebles_en_pagina == LIMIT_PER_PAGE:
        # Lógica de reintento para superar fallos de servidor de Domus
        retries = 0
        success = False
        data = None
        
        while retries < MAX_RETRIES and not success:
            try:
                # Construir los parámetros de la petición
                params = {
                    'token': DOMUS_TOKEN,
                    'estado': 'Disponible',
                    'limit': LIMIT_PER_PAGE,
                    'page': page
                }
                
                print(f"Página: {page} - Intentando conexión (Intento: {retries + 1})...")
                
                # Petición a la API
                response = requests.get(DOMUS_API_BASE, params=params, timeout=30) # 30s de timeout
                response.raise_for_status() # Lanza un error para códigos de estado 4xx/5xx
                
                data = response.json()
                success = True # Éxito en la conexión
                
            except requests.exceptions.RequestException as e:
                # Manejo de errores de timeout o conexión
                retries += 1
                delay = RETRY_DELAY * (2 ** (retries - 1)) # Retraso exponencial
                print(f"Error en Pág {page}: {e}. Reintentando en {delay}s...")
                time.sleep(delay)
            except Exception as e:
                # Otros errores
                return {'statusCode': 500, 'body': f"Error inesperado en Pág {page}: {e}"}

        if not success:
            # Si se excede el número de reintentos, se termina el proceso
            return {'statusCode': 500, 'body': f"Error crítico: Fallo de conexión con Domus después de {MAX_RETRIES} intentos en la Página {page}."}
        
        
        # --- 3. GUARDAR EN SUPABASE ---
        inmuebles = data.get('inmuebles', [])
        inmuebles_en_pagina = len(inmuebles)
        
        if inmuebles_en_pagina > 0:
            print(f"Página {page}: {inmuebles_en_pagina} inmuebles recibidos. Insertando en DB...")
            
            try:
                # Conexión a la base de datos
                conn = psycopg2.connect(**db_params)
                cursor = conn.cursor()
                
                # Crear la lista de inmuebles a insertar
                inmuebles_a_insertar = []
                for inm in inmuebles:
                    # Usamos 'codpro' como clave única y 'inmueble_data' para guardar todo el JSON
                    codpro = inm.get('codpro')
                    # Usar json.dumps para formatear el JSON para PostgreSQL
                    inm_json = json.dumps(inm) 
                    
                    # Preparar la tupla (codpro, inmueble_data, estado_domus, fecha_actualizacion)
                    # La fecha de actualización la tomamos directamente de la respuesta (o la generamos aquí)
                    # En este caso, usaremos el campo del JSON si existe, o la fecha actual si no.
                    fecha_act = inm.get('fecha_actualizacion')
                    
                    inmuebles_a_insertar.append((
                        codpro, 
                        inm_json, 
                        inm.get('estado'), 
                        fecha_act
                    ))

                # Query para la inserción masiva con UPSERT (INSERT O UPDATE)
                # Esto es la clave: si 'codpro' ya existe, actualiza los campos; si no, inserta.
                query = """
                INSERT INTO inmuebles_domus (codpro, inmueble_data, estado_domus, fecha_actualizacion, sincronizado_wp)
                VALUES (%s, %s, %s, %s, FALSE)
                ON CONFLICT (codpro) 
                DO UPDATE SET 
                    inmueble_data = EXCLUDED.inmueble_data,
                    estado_domus = EXCLUDED.estado_domus,
                    fecha_actualizacion = EXCLUDED.fecha_actualizacion,
                    # Si la data cambia, marcamos como NO sincronizado para forzar la actualización en WP
                    sincronizado_wp = FALSE
                """
                
                # Ejecutar la inserción masiva
                cursor.executemany(query, inmuebles_a_insertar)
                conn.commit()
                
                total_inmuebles_actualizados += inmuebles_en_pagina
                print(f"Página {page}: {inmuebles_en_pagina} inmuebles guardados/actualizados.")

            except Exception as e:
                print(f"Error de base de datos en Pág {page}: {e}")
                # En caso de error de DB, hacemos ROLLBACK y podemos terminar el proceso o continuar
                if conn: conn.rollback() 
                return {'statusCode': 500, 'body': f"Error de DB en página {page}: {e}"}
            finally:
                if cursor: cursor.close()
                if conn: conn.close()
            
            page += 1
        else:
            # Si la API no retorna inmuebles, se asume que se llegó al final de la paginación
            print("Fin de la paginación.")
            break

    # --- 4. RESPUESTA FINAL ---
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': 'Proceso de extracción y actualización finalizado con éxito.',
            'total_inmuebles_procesados': total_inmuebles_actualizados,
            'paginas_recorridas': page - 1
        })
    }
