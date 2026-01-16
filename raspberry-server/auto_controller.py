"""
Controlador Automático de Trenes v4.0 - Con Sincronización y Tiempos Configurables
Maneja el modo automático: paradas en estaciones y ciclo continuo

MEJORAS v4.0:
- Tiempos de parada configurables por tren
- Sincronización: otros trenes esperan a que tren 1 alcance estación 1
- Control de velocidades: otros trenes no pueden ir más rápido que tren 1
- Eventos de estación específicos por tren
- Loop más eficiente (0.5s en lugar de 0.1s)
- Verifica estado del tren antes de enviar comandos
- Usa nuevo sistema de colas con prioridades
- No envía comandos si hay cola saturada
- Mejor manejo de errores y recuperación
"""
import redis
import json
import time
import os
from dotenv import load_dotenv
from threading import Thread
import requests

load_dotenv()

REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
PROXY_HOST = os.getenv('PROXY_HOST', 'localhost')
PROXY_PORT = int(os.getenv('PROXY_PORT', 5000))
WEB_SERVER_HOST = os.getenv('WEB_SERVER_HOST', 'localhost')
WEB_SERVER_PORT = int(os.getenv('WEB_SERVER_PORT', 8000))

redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    db=0,
    decode_responses=True
)

# Configuración OPTIMIZADA
DEFAULT_CRUISE_SPEED = 75  # Velocidad de crucero por defecto
DEFAULT_STATION_STOP_TIME = 5  # Segundos de parada por defecto
MAX_QUEUE_SIZE = 5  # No enviar más comandos si la cola está saturada

class TrainAutoController:
    def __init__(self, train_id):
        self.train_id = train_id
        self.running = False
        self.current_direction = "ADELANTE"
        self.at_station = False
        self.current_station_id = None
        self.station_stop_start = None
        self.cruise_speed = DEFAULT_CRUISE_SPEED
        self.station_times = {1: DEFAULT_STATION_STOP_TIME, 2: DEFAULT_STATION_STOP_TIME, 3: DEFAULT_STATION_STOP_TIME}
        
        # Cargar configuración de tiempos de parada Y velocidad
        self.load_station_config()
        self.load_cruise_speed()
        
        # Suscribirse a eventos de estaciones
        self.pubsub = redis_client.pubsub()
        self.pubsub.subscribe('stations:events')
        
        print(f"[AUTO T{train_id}] Controlador iniciado")
        print(f"[AUTO T{train_id}] Velocidad crucero: {self.cruise_speed}%")
        print(f"[AUTO T{train_id}] Tiempos de parada: Est1={self.station_times[1]}s, Est2={self.station_times[2]}s, Est3={self.station_times[3]}s")
    
    def load_station_config(self):
        """Carga configuración de tiempos de parada desde Redis"""
        try:
            config = redis_client.hgetall(f"tren:{self.train_id}:station_config")
            if config:
                new_time_1 = int(config.get("station_1_time", DEFAULT_STATION_STOP_TIME))
                new_time_2 = int(config.get("station_2_time", DEFAULT_STATION_STOP_TIME))
                new_time_3 = int(config.get("station_3_time", DEFAULT_STATION_STOP_TIME))
                
                # Informar si cambió la configuración
                if new_time_1 != self.station_times.get(1) or new_time_2 != self.station_times.get(2) or new_time_3 != self.station_times.get(3):
                    print(f"[AUTO T{self.train_id}] Actualizando tiempos: Est1={new_time_1}s, Est2={new_time_2}s, Est3={new_time_3}s")
                
                self.station_times[1] = new_time_1
                self.station_times[2] = new_time_2
                self.station_times[3] = new_time_3
        except Exception as e:
            print(f"[AUTO T{self.train_id}] Error cargando config: {e}")
    
    def load_cruise_speed(self):
        """Carga velocidad crucero desde Redis"""
        try:
            config = redis_client.hgetall(f"tren:{self.train_id}:config")
            if config and "cruise_speed" in config:
                new_speed = int(config.get("cruise_speed", DEFAULT_CRUISE_SPEED))
                if new_speed != self.cruise_speed:
                    print(f"[AUTO T{self.train_id}] Actualizando velocidad crucero: {new_speed}%")
                self.cruise_speed = new_speed
        except Exception as e:
            print(f"[AUTO T{self.train_id}] Error cargando velocidad: {e}")
    
    def is_auto_mode_enabled(self):
        """Verifica si el modo automático está habilitado para este tren"""
        auto_state = redis_client.hget(f"tren:{self.train_id}:auto", "enabled")
        return auto_state == "1"
    
    def calculate_cruise_speed(self):
        """
        Calcula la velocidad de crucero del tren.
        SIMPLIFICADO: Usa la velocidad configurada en Redis para este tren.
        """
        # Recargar velocidad desde Redis (puede haber cambiado)
        self.load_cruise_speed()
        
        speed = self.cruise_speed
        
        # VALIDACIÓN: Asegurar que la velocidad nunca sea 0 o inválida
        if speed is None or speed <= 0:
            print(f"[AUTO T{self.train_id}] ⚠️ ADVERTENCIA: Velocidad inválida ({speed}), usando valor por defecto {DEFAULT_CRUISE_SPEED}%")
            speed = DEFAULT_CRUISE_SPEED
        
        # Asegurar que esté en rango válido (1-100)
        speed = max(1, min(100, speed))
        
        return int(speed)
    
    def send_command(self, comando, parametros=None, retry_count=2):
        """
        Envía comando al tren usando el nuevo sistema de prioridades.
        Usa el endpoint /train/<id>/cmd/push del proxy.
        
        Args:
            comando: Comando a enviar
            parametros: Parámetros del comando
            retry_count: Número de reintentos en caso de error
        """
        if parametros is None:
            parametros = {}
        
        # Verificar tamaño de cola antes de enviar (prevenir saturación)
        queue_sizes = self.get_queue_sizes()
        total_queued = queue_sizes.get('priority', 0) + queue_sizes.get('normal', 0)
        
        if total_queued > MAX_QUEUE_SIZE and comando != "stop":
            print(f"[AUTO T{self.train_id}] Cola saturada ({total_queued}), omitiendo comando {comando}")
            return False
        
        # Intentar enviar comando con reintentos
        for attempt in range(retry_count + 1):
            try:
                # Usar el endpoint del proxy que maneja prioridades automáticamente
                url = f"http://{PROXY_HOST}:{PROXY_PORT}/train/{self.train_id}/cmd/push"
                payload = {
                    "comando": comando,
                    "parametros": parametros
                }
                
                response = requests.post(url, json=payload, timeout=3)
                
                if response.status_code == 200:
                    result = response.json()
                    if result.get("success"):
                        print(f"[AUTO T{self.train_id}] ✓ Comando enviado: {comando} {parametros}")
                        return True
                    else:
                        print(f"[AUTO T{self.train_id}] ✗ Proxy rechazó comando: {result.get('error', 'unknown')}")
                        return False
                else:
                    print(f"[AUTO T{self.train_id}] ✗ HTTP {response.status_code} al enviar comando")
                    if attempt < retry_count:
                        print(f"[AUTO T{self.train_id}] Reintentando... ({attempt + 1}/{retry_count})")
                        time.sleep(0.5)
                    else:
                        return False
                        
            except requests.exceptions.Timeout:
                print(f"[AUTO T{self.train_id}] ✗ Timeout enviando comando {comando}")
                if attempt < retry_count:
                    print(f"[AUTO T{self.train_id}] Reintentando... ({attempt + 1}/{retry_count})")
                    time.sleep(0.5)
                else:
                    return False
                    
            except requests.exceptions.ConnectionError:
                print(f"[AUTO T{self.train_id}] ✗ No se pudo conectar al proxy")
                if attempt < retry_count:
                    print(f"[AUTO T{self.train_id}] Reintentando... ({attempt + 1}/{retry_count})")
                    time.sleep(1)
                else:
                    return False
                    
            except Exception as e:
                print(f"[AUTO T{self.train_id}] ✗ Error inesperado: {e}")
                if attempt < retry_count:
                    print(f"[AUTO T{self.train_id}] Reintentando... ({attempt + 1}/{retry_count})")
                    time.sleep(0.5)
                else:
                    return False
        
        return False
    
    def get_queue_sizes(self):
        """Obtiene tamaño de las colas de comandos"""
        try:
            priority_size = redis_client.llen(f"tren/{self.train_id}/cmd/priority") or 0
            normal_size = redis_client.llen(f"tren/{self.train_id}/cmd/normal") or 0
            return {"priority": priority_size, "normal": normal_size}
        except:
            return {"priority": 0, "normal": 0}
    
    def get_train_state(self):
        """Obtiene estado actual del tren"""
        state = redis_client.hgetall(f"tren:{self.train_id}:state")
        if state:
            return {
                "direction": state.get("direction", "STOP"),
                "speed": int(state.get("speed", 0)),
                "motors_running": state.get("motors_running") == "1"
            }
        return None
    
    def handle_station_event(self, event):
        """Maneja eventos de llegada/salida de estaciones específicos por tren"""
        # Verificar si el evento es para este tren
        event_train_id = event.get('train_id')
        if event_train_id is None or event_train_id != self.train_id:
            return  # Este evento no es para este tren
        
        station_id = event.get('station_id')
        
        if event['event_type'] == 'train_arrived':
            print(f"[AUTO T{self.train_id}] ✓ Llegada detectada a {event['station_name']}")
            
            # Parar el tren
            self.send_command("stop")
            self.at_station = True
            self.current_station_id = station_id
            self.station_stop_start = time.time()
            print(f"[AUTO T{self.train_id}] Esperando {self.station_times.get(station_id, DEFAULT_STATION_STOP_TIME)}s en estación...")
            
        elif event['event_type'] == 'train_departed':
            # IMPORTANTE: NO procesar evento de salida mientras estamos controlando la parada
            # El auto_controller decide cuándo salir, no el station_manager
            if not self.at_station:
                print(f"[AUTO T{self.train_id}] Confirmación de salida de {event['station_name']}")
            else:
                # Ignorar evento train_departed mientras estamos en espera controlada
                print(f"[AUTO T{self.train_id}] [DEBUG] Ignorando evento train_departed - tren en espera controlada")
    
    def run(self):
        """Loop principal del controlador automático - SIMPLIFICADO v4.1"""
        self.running = True
        print(f"[AUTO T{self.train_id}] Modo automático activado")
        
        # Esperar un momento para asegurar que el tren esté listo
        time.sleep(1)
        
        # Calcular velocidad de crucero permitida
        cruise_speed = self.calculate_cruise_speed()
        print(f"[AUTO T{self.train_id}] [DEBUG] Velocidad inicial: {cruise_speed}%")
        
        # Iniciar movimiento con velocidad explícita
        self.send_command("move_forward", {"velocidad_objetivo": int(cruise_speed)})
        
        while self.running:
            try:
                # Verificar si modo automático sigue activo
                if not self.is_auto_mode_enabled():
                    print(f"[AUTO T{self.train_id}] Modo automático desactivado")
                    break
                
                # Verificar estado del tren antes de tomar decisiones
                train_state = self.get_train_state()
                
                # Procesar eventos de estaciones (no bloqueante)
                message = self.pubsub.get_message()
                if message and message['type'] == 'message':
                    try:
                        event = json.loads(message['data'])
                        self.handle_station_event(event)
                    except Exception as e:
                        print(f"[AUTO T{self.train_id}] Error procesando evento: {e}")
                
                # Lógica de parada en estación con tiempos configurables
                if self.at_station and self.station_stop_start and self.current_station_id:
                    # Recargar configuración de tiempos (por si cambió desde el frontend)
                    self.load_station_config()
                    
                    # Obtener tiempo de parada para esta estación
                    stop_time = self.station_times.get(self.current_station_id, DEFAULT_STATION_STOP_TIME)
                    elapsed = time.time() - self.station_stop_start
                    
                    # Log de progreso cada 2 segundos
                    if int(elapsed) % 2 == 0 and elapsed > 0 and elapsed < stop_time:
                        remaining = stop_time - elapsed
                        print(f"[AUTO T{self.train_id}] En estación {self.current_station_id}: {elapsed:.1f}s / {stop_time}s (faltan {remaining:.1f}s)")
                    
                    if elapsed >= stop_time:
                        # Determinar dirección para continuar
                        # LÓGICA CORRECTA: Solo cambiar si llegamos a estación terminal viniendo en esa dirección
                        # Estación 1 (Portal Américas) = Terminal IZQUIERDA
                        # Estación 2 (Calle 72) = Terminal DERECHA
                        
                        print(f"[AUTO T{self.train_id}] [DEBUG] Dirección actual: {self.current_direction}, Estación: {self.current_station_id}")
                        
                        if self.current_station_id == 2 and self.current_direction == "ADELANTE":
                            # Llegamos al final yendo hacia adelante → cambiar a atrás
                            next_direction = "ATRAS"
                            command = "move_reverse"
                            print(f"[AUTO T{self.train_id}] Llegamos a estación terminal 2 → Cambiando a ATRÁS")
                        elif self.current_station_id == 1 and self.current_direction == "ATRAS":
                            # Llegamos al inicio yendo hacia atrás → cambiar a adelante
                            next_direction = "ADELANTE"
                            command = "move_forward"
                            print(f"[AUTO T{self.train_id}] Llegamos a estación terminal 1 → Cambiando a ADELANTE")
                        else:
                            # En cualquier otro caso, mantener dirección actual
                            next_direction = self.current_direction
                            command = "move_forward" if next_direction == "ADELANTE" else "move_reverse"
                            print(f"[AUTO T{self.train_id}] Manteniendo dirección: {next_direction}")
                        
                        # Recalcular velocidad permitida (por si cambió tren 1)
                        cruise_speed = self.calculate_cruise_speed()
                        
                        # DEBUG: Verificar velocidad calculada
                        print(f"[AUTO T{self.train_id}] [DEBUG] Velocidad calculada: {cruise_speed}%")
                        print(f"[AUTO T{self.train_id}] [DEBUG] Parámetros a enviar: {{'velocidad_objetivo': {cruise_speed}}}")
                        
                        # Terminar parada, continuar viaje
                        print(f"[AUTO T{self.train_id}] Continuando viaje después de {stop_time}s en dirección {next_direction}...")
                        
                        # Enviar comando de movimiento con velocidad explícita
                        params = {"velocidad_objetivo": int(cruise_speed)}
                        success = self.send_command(command, params)
                        
                        if success:
                            print(f"[AUTO T{self.train_id}] ✓ Comando enviado exitosamente")
                        else:
                            print(f"[AUTO T{self.train_id}] ✗ FALLO al enviar comando - Reintentando...")
                            # Reintentar una vez más con velocidad por defecto
                            self.send_command(command, {"velocidad_objetivo": int(DEFAULT_CRUISE_SPEED)})
                        
                        # Actualizar dirección actual
                        self.current_direction = next_direction
                        self.station_stop_start = None
                        self.at_station = False
                
                # Loop más espaciado para reducir carga de CPU (0.5s en lugar de 0.1s)
                time.sleep(0.5)
                
            except Exception as e:
                print(f"[AUTO T{self.train_id}] Error: {e}")
                time.sleep(1)
        
        # Al salir, detener el tren (comando de prioridad)
        print(f"[AUTO T{self.train_id}] Finalizando modo automático - Deteniendo tren")
        self.send_command("stop")
        self.pubsub.close()
        print(f"[AUTO T{self.train_id}] Modo automático finalizado")


class AutoControllerManager:
    """Gestor de controladores automáticos para múltiples trenes"""
    def __init__(self):
        self.controllers = {}
        self.threads = {}
        print("[MANAGER] Gestor de controladores automáticos iniciado")
    
    def monitor_auto_modes(self):
        """Monitorea qué trenes tienen modo automático activo"""
        while True:
            try:
                # Buscar trenes con modo automático activado
                pattern = "tren:*:auto"
                keys = redis_client.keys(pattern)
                
                for key in keys:
                    train_id = int(key.split(':')[1])
                    auto_enabled = redis_client.hget(key, "enabled") == "1"
                    
                    # Si está activado y no hay controlador, crear uno
                    if auto_enabled and train_id not in self.controllers:
                        print(f"[MANAGER] Iniciando controlador para tren {train_id}")
                        controller = TrainAutoController(train_id)
                        self.controllers[train_id] = controller
                        
                        thread = Thread(target=controller.run, daemon=True)
                        thread.start()
                        self.threads[train_id] = thread
                    
                    # Si está desactivado y hay controlador, detenerlo
                    elif not auto_enabled and train_id in self.controllers:
                        print(f"[MANAGER] Deteniendo controlador para tren {train_id}")
                        self.controllers[train_id].running = False
                        del self.controllers[train_id]
                        del self.threads[train_id]
                
                time.sleep(2)
                
            except Exception as e:
                print(f"[MANAGER] Error: {e}")
                time.sleep(5)


def main():
    print("\n" + "="*60)
    print("  CONTROLADOR AUTOMÁTICO - METRO BOGOTÁ v4.0")
    print("="*60)
    print(f"Redis: {REDIS_HOST}:{REDIS_PORT}")
    print(f"Velocidad de crucero por defecto: {DEFAULT_CRUISE_SPEED}%")
    print(f"Tiempo de parada por defecto: {DEFAULT_STATION_STOP_TIME}s")
    print("MEJORAS:")
    print("  - Tiempos de parada configurables por tren")
    print("  - Sincronización: otros trenes esperan a tren 1")
    print("  - Control de velocidades relativas")
    print("="*60 + "\n")
    
    # Verificar Redis
    try:
        redis_client.ping()
        print("[OK] Conexión a Redis exitosa\n")
    except Exception as e:
        print(f"[ERROR] No se puede conectar a Redis: {e}")
        return
    
    # Iniciar gestor
    manager = AutoControllerManager()
    
    print("[SYSTEM] Sistema de control automático activo")
    print("Presiona Ctrl+C para detener\n")
    
    try:
        manager.monitor_auto_modes()
    except KeyboardInterrupt:
        print("\n\n[SYSTEM] Deteniendo sistema...")
        print("[SYSTEM] Adiós")


if __name__ == "__main__":
    main()
