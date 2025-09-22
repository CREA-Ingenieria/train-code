import time
import json
import redis

TRAIN_ID = 1

class Train:
    """
    Clase que simula el comportamiento del tren.
    En lugar de mover motores, imprime las acciones en la consola.
    """
    def __init__(self):
        self.current_speed = 0
        self.direction = "detenido"
        print("Simulador de Tren inicializado.")

    def move_forward(self, target_speed):
        print(f"SIMULADOR: Moviendo hacia ADELANTE. Velocidad objetivo: {target_speed}%.")
        self.current_speed = target_speed
        self.direction = "adelante"

    def move_reverse(self, target_speed):
        print(f"SIMULADOR: Moviendo en REVERSA. Velocidad objetivo: {target_speed}%.")
        self.current_speed = target_speed
        self.direction = "reversa"

    def stop(self):
        print("SIMULADOR: Tren DETENIDO.")
        self.current_speed = 0
        self.direction = "detenido"

def process_command(train, command_data):
    """Parsea el JSON y ejecuta la acción correspondiente."""
    try:
        message = json.loads(command_data)
        command = message.get("comando")
        params = message.get("parametros", {})

        print(f"-> Comando recibido: {command}, Parámetros: {params}")

        if command == "move_forward":
            speed = params.get("velocidad_objetivo", 75)
            train.move_forward(speed)
        elif command == "move_reverse":
            speed = params.get("velocidad_objetivo", 75)
            train.move_reverse(speed)
        elif command == "stop":
            train.stop()
        else:
            print(f"Comando desconocido: {command}")

    except Exception as e:
        print(f"Error procesando el comando: {e}")

def main():
    redis_host='localhost'
    redis_port=6379
    
    metro_bogota = Train()
    
    # Verificar conexión a Redis antes de empezar
    try:
        redis_client = redis.Redis(host=redis_host, port=redis_port, db=0, decode_responses=True)
        redis_client.ping()  
        print(f"Conectado a Redis en {redis_host}:{redis_port}")
    
    except redis.ConnectionError:
        print(f"No se puede conectar a Redis en {redis_host}:{redis_port}")
        print("Asegúrate de que Redis esté ejecutándose.")
        return
    
    channel = f"tren/{TRAIN_ID}/cmd"
    print(f"\n Train/ESP32 listo. Suscribiéndose al canal: '{channel}'")
    
    while True:
        try:
            response = redis_client.blpop(channel, 0)
            if response:
                command_str = response[1]
                process_command(metro_bogota, command_str)
        
        except redis.ConnectionError as e:
            print(f"Perdida conexión con Redis: {e}")
            print("Intentando reconectar en 5 segundos...")
            time.sleep(5)
            
            try:
                redis_client = redis.Redis(host=redis_host, port=redis_port, db=0, decode_responses=True)
                redis_client.ping()
                print("Reconexión exitosa.")
            
            except:
                print("Fallo en la reconexión.")
        
        except Exception as e:
            print(f"Error inesperado: {e}")
            time.sleep(1)

if __name__ == "__main__":
    main()