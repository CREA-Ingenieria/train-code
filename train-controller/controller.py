import redis
import json
import time

class TrainAPI:
    """
    Clase para controlar el tren enviando comandos a una lista de Redis.
    """
    def __init__(self, redis_host='localhost', redis_port=6379):
        """
        Inicializa la conexión con el servidor Redis.
        """
        self.redis_client = redis.Redis(host=redis_host, port=redis_port, db=0)
        print("-> Conectado al servidor Redis.")

    def send_command(self, train_id: int, command: str, params: dict = None):
        """
        Construye y envía un comando a la lista de Redis del tren.
        """
        if params is None:
            params = {}
            
        channel = f"tren/{train_id}/cmd"
        message = {
            "comando": command,
            "parametros": params
        }
        
        payload = json.dumps(message)
        
        print(f"-> ENVIANDO a lista '{channel}': {payload}")
        self.redis_client.lpush(channel, payload)
        print(f"Comando agregado a la lista")

# --- Ejemplo de uso ---
if __name__ == "__main__":
    # Suponemos que el ID del tren es 1
    TRAIN_ID = 1
    
    # Inicializamos la API
    api = TrainAPI()
    
    print("\n--- Iniciando prueba de la API del Tren ---")

    # 1. Mover el tren hacia adelante al 80% de velocidad
    api.send_command(TRAIN_ID, "move_forward", {"velocidad_objetivo": 80})
    time.sleep(5) # Esperar 5 segundos

    # 2. Detener el tren
    api.send_command(TRAIN_ID, "stop")
    time.sleep(3) # Esperar 3 segundos

    # 3. Mover el tren en reversa al 50% de velocidad
    api.send_command(TRAIN_ID, "move_reverse", {"velocidad_objetivo": 50})
    time.sleep(5)

    # 4. Detener el tren nuevamente
    api.send_command(TRAIN_ID, "stop")

    print("--- Prueba finalizada ---")