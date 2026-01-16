"""
Gestor de Estaciones v4.0 - Con Detección de Tren Específico
Detecta trenes en estaciones y publica eventos a Redis

MEJORAS v4.0:
- Identifica qué tren específico llega a cada estación (por orden)
- Sincronización con sistema de trenes (tren 1 primero)
- Mediciones más espaciadas para reducir carga de CPU
- Filtrado de ruido con promedio móvil
- Debounce mejorado con historial de lecturas
- Reducción de eventos falsos positivos
"""
import RPi.GPIO as GPIO
import time
import redis
import json
from threading import Thread, Lock
from collections import deque
import os
from dotenv import load_dotenv

load_dotenv()

# Configuración GPIO
GPIO.setmode(GPIO.BCM)
GPIO.setwarnings(False)

# Pines de sensores ultrasónicos
STATIONS = {
    1: {
        "name": "Portal Américas",
        "trig": 23,
        "echo": 24,
        "led_near": 10,
        "led_far": 22,
        "position": "start"  # arriba-izquierda
    },
    2: {
        "name": "Calle 72",
        "trig": 5,
        "echo": 6,
        "led_near": 17,
        "led_far": 27,
        "position": "end"  # abajo-derecha
    },
    3: {
        "name": "Estación 3",
        "trig": 19,
        "echo": 26,
        "led_near": 13,
        "led_far": 12,
        "position": "middle"  # posición intermedia
    }
}

# Umbrales OPTIMIZADOS
NEAR_CM = 11     # Disparo de "cerca"
FAR_CM = 22      # Para soltar la condición
MEASURE_PERIOD = 0.15  # 300ms - REDUCIDO de 150ms para menor carga
COOLDOWN = 3.0    # 2s - AUMENTADO para evitar eventos duplicados
SAMPLES = 4       # Promedio de 3 lecturas para filtrar ruido

# Redis
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)

# Sistema de cola de trenes POR ESTACIÓN (cada estación tiene su propia cola)
station_queues = {}  # {station_id: [train_ids...]}
station_queue_lock = Lock()

# Orden por defecto de los trenes (secuencia base)
DEFAULT_QUEUE_ORDER = [1, 2]

# Tiempo máximo (segundos) para considerar que un tren sigue conectado vía heartbeat
HEARTBEAT_TIMEOUT = 12

class StationSensor:
    def __init__(self, station_id, config):
        self.station_id = station_id
        self.config = config
        self.train_detected = False
        self.current_train_id = None  # ID del tren actualmente en la estación
        self.last_trigger_time = 0
        self.waiting_departure = False  # Flag simple para esperar salida
        self.departure_wait_start = 0   # Cuándo empezó a esperar salida
        
        # Cola de trenes independiente para esta estación
        with station_queue_lock:
            if station_id not in station_queues:
                station_queues[station_id] = DEFAULT_QUEUE_ORDER.copy()
            elif not station_queues[station_id]:
                station_queues[station_id] = DEFAULT_QUEUE_ORDER.copy()
        
        # Buffer de lecturas para filtrado de ruido
        self.distance_buffer = deque(maxlen=SAMPLES)
        
        # Configurar pines
        GPIO.setup(config['trig'], GPIO.OUT)
        GPIO.setup(config['echo'], GPIO.IN)
        GPIO.setup(config['led_near'], GPIO.OUT)
        GPIO.setup(config['led_far'], GPIO.OUT)
        
        # Apagar LEDs
        GPIO.output(config['led_near'], GPIO.LOW)
        GPIO.output(config['led_far'], GPIO.LOW)
        
        print(f"[STATION {station_id}] Inicializado: {config['name']}")
    
    def measure_distance(self):
        """Mide distancia con sensor ultrasónico"""
        try:
            # Pulso trigger
            GPIO.output(self.config['trig'], GPIO.LOW)
            time.sleep(0.002)
            GPIO.output(self.config['trig'], GPIO.HIGH)
            time.sleep(0.00001)
            GPIO.output(self.config['trig'], GPIO.LOW)
            
            # Medir echo
            timeout = time.time() + 0.1  # Timeout 100ms
            
            pulse_start = time.time()
            while GPIO.input(self.config['echo']) == GPIO.LOW:
                pulse_start = time.time()
                if pulse_start > timeout:
                    return None
            
            pulse_end = time.time()
            while GPIO.input(self.config['echo']) == GPIO.HIGH:
                pulse_end = time.time()
                if pulse_end > timeout:
                    return None
            
            pulse_duration = pulse_end - pulse_start
            distance = pulse_duration * 17150  # cm
            distance = round(distance, 2)
            
            return distance if distance < 400 else None
            
        except Exception as e:
            print(f"[STATION {self.station_id}] Error midiendo: {e}")
            return None
    
    def get_filtered_distance(self):
        """Obtiene distancia filtrada usando promedio móvil"""
        raw_distance = self.measure_distance()
        
        if raw_distance is None:
            return None
        
        # Agregar al buffer
        self.distance_buffer.append(raw_distance)
        
        # Si no tenemos suficientes muestras, devolver la cruda
        if len(self.distance_buffer) < SAMPLES:
            return raw_distance
        
        # Calcular promedio (filtra ruido)
        filtered = sum(self.distance_buffer) / len(self.distance_buffer)
        return round(filtered, 2)
    
    def update_leds(self, distance):
        """Actualiza LEDs según distancia"""
        if distance is None or distance > FAR_CM:
            # Lejos o sin detección
            GPIO.output(self.config['led_near'], GPIO.LOW)
            GPIO.output(self.config['led_far'], GPIO.LOW)
            return False
        elif distance <= NEAR_CM:
            # Cerca - tren en estación
            GPIO.output(self.config['led_near'], GPIO.HIGH)
            GPIO.output(self.config['led_far'], GPIO.LOW)
            return True
        else:
            # Rango medio
            GPIO.output(self.config['led_near'], GPIO.LOW)
            GPIO.output(self.config['led_far'], GPIO.HIGH)
            return False
    
    def get_next_train_in_queue(self):
        """
        LÓGICA SIMPLE:
        - Si cola vacía → retorna 1
        - Si cola tiene elementos → toma el primero y lo rota al final
        """
        with station_queue_lock:
            # Asegurar que la cola existe
            if self.station_id not in station_queues:
                station_queues[self.station_id] = []
            
            # Si cola vacía → tren 1 por defecto
            if len(station_queues[self.station_id]) == 0:
                print(f"[STATION {self.station_id}] Cola vacía → TREN 1")
                return 1
            
            # Si solo hay 1 tren → siempre retornar ese
            if len(station_queues[self.station_id]) == 1:
                train_id = station_queues[self.station_id][0]
                print(f"[STATION {self.station_id}] Solo 1 tren → TREN {train_id}")
                return train_id
            
            # Si hay 2+ trenes → rotar
            train_id = station_queues[self.station_id].pop(0)
            station_queues[self.station_id].append(train_id)
            print(f"[STATION {self.station_id}] Rotado → TREN {train_id} | Cola ahora: {station_queues[self.station_id]}")
            return train_id
    
    def publish_event(self, event_type, distance, train_id=None):
        """Publica evento a Redis con identificación del tren"""
        event = {
            "station_id": self.station_id,
            "station_name": self.config['name'],
            "event_type": event_type,
            "distance": distance,
            "timestamp": time.time(),
            "position": self.config['position'],
            "train_id": train_id
        }
        
        # Publicar al canal de eventos de estaciones
        redis_client.publish("stations:events", json.dumps(event))
        
        # Guardar en hash de estado de estación
        redis_client.hset(
            f"station:{self.station_id}:state",
            mapping={
                "train_present": "1" if event_type == "train_arrived" else "0",
                "train_id": str(train_id) if train_id else "0",
                "distance": distance if distance else 0,
                "last_update": time.time()
            }
        )
        redis_client.expire(f"station:{self.station_id}:state", 10)
        
        # Si es estación 1 y tren 1 llegó, actualizar config del sistema
        if self.station_id == 1 and train_id == 1 and event_type == "train_arrived":
            redis_client.hset("system:config", "train1_reached_station1", "1")
            print(f"[STATION {self.station_id}] ✓ TREN 1 ALCANZÓ ESTACIÓN 1 - Desbloqueando otros trenes")
        
        train_info = f" (Tren #{train_id})" if train_id else ""
        print(f"[STATION {self.station_id}] {event_type.upper()}{train_info}: {distance}cm")
    
    def run(self):
        """Loop principal del sensor - Versión mejorada con detección robusta"""
        consecutive_detections = 0
        consecutive_absences = 0
        REQUIRED_DETECTIONS = 2  # Requerir 2 lecturas consecutivas para confirmar
        DEPARTURE_WAIT_TIME = 1.0  # Esperar 1 segundo antes de confirmar salida
        
        while True:
            try:
                # Usar distancia filtrada para reducir ruido
                distance = self.get_filtered_distance()
                train_present = self.update_leds(distance)
                
                current_time = time.time()
                
                # Debounce: requerir lecturas consecutivas
                if train_present:
                    consecutive_detections += 1
                    consecutive_absences = 0
                    # Si estaba esperando salida, cancelar
                    if self.waiting_departure:
                        print(f"[STATION {self.station_id}] Cancelando salida - tren aún presente ({distance}cm)")
                        self.waiting_departure = False
                        self.departure_wait_start = 0
                else:
                    consecutive_absences += 1
                    consecutive_detections = 0
                
                # Detectar llegada de tren
                if consecutive_detections >= REQUIRED_DETECTIONS and not self.train_detected:
                    if current_time - self.last_trigger_time > COOLDOWN:
                        # Obtener el siguiente tren en la cola
                        self.current_train_id = self.get_next_train_in_queue()
                        
                        self.train_detected = True
                        self.last_trigger_time = current_time
                        self.waiting_departure = False  # Reset
                        self.departure_wait_start = 0   # Reset
                        self.publish_event("train_arrived", distance, self.current_train_id)
                        consecutive_detections = 0
                        print(f"[STATION {self.station_id}] Tren #{self.current_train_id} detectado en estación ({distance}cm)")
                
                # Detectar salida de tren - CON ESPERA Y CONFIRMACIÓN
                elif consecutive_absences >= REQUIRED_DETECTIONS and self.train_detected:
                    # Si no estamos esperando, iniciar espera
                    if not self.waiting_departure:
                        self.waiting_departure = True
                        self.departure_wait_start = current_time
                        print(f"[STATION {self.station_id}] Posible salida de tren #{self.current_train_id} - esperando confirmación ({DEPARTURE_WAIT_TIME}s)...")
                    
                    # Si ya estamos esperando, verificar si pasó el tiempo
                    elif self.departure_wait_start > 0 and (current_time - self.departure_wait_start >= DEPARTURE_WAIT_TIME):
                        # Confirmar salida después del tiempo de espera
                        elapsed = current_time - self.departure_wait_start
                        print(f"[STATION {self.station_id}] Salida de tren #{self.current_train_id} confirmada después de {elapsed:.1f}s sin detección")
                        
                        self.train_detected = False
                        self.waiting_departure = False
                        self.departure_wait_start = 0
                        self.publish_event("train_departed", distance, self.current_train_id)
                        self.current_train_id = None
                        consecutive_absences = 0
                
                time.sleep(MEASURE_PERIOD)
                
            except Exception as e:
                print(f"[STATION {self.station_id}] Error en loop: {e}")
                time.sleep(1)


def update_train_queue():
    """
    LÓGICA SIMPLE DE COLAS:
    - Al inicio: todas las estaciones esperan [1, 2]
    - Si solo tren 1 está activo: cola = [1]
    - Si solo tren 2 está activo: cola = [2]
    - Si ambos están activos: cola = [1, 2]
    - Si ninguno está activo: cola = [] (get_next_train retorna 1 por defecto)
    """
    
    while True:
        try:
            # Reset de colas
            reset_flag = redis_client.get("system:reset_station_queues")
            if reset_flag == "1":
                print("[QUEUE] Reset solicitado - Reiniciando colas")
                with station_queue_lock:
                    for station_id in station_queues.keys():
                        station_queues[station_id] = DEFAULT_QUEUE_ORDER.copy()
                        print(f"[QUEUE] Est.{station_id} reiniciada → {station_queues[station_id]}")
                redis_client.delete("system:reset_station_queues")
            
            # Determinar trenes conectados usando heartbeats recientes
            connected_trains = []
            now = time.time()
            for train_id in DEFAULT_QUEUE_ORDER:
                heartbeat_key = f"tren:{train_id}:heartbeat"
                heartbeat_value = redis_client.get(heartbeat_key)
                if heartbeat_value:
                    try:
                        seconds_ago = now - float(heartbeat_value)
                    except (ValueError, TypeError):
                        seconds_ago = HEARTBEAT_TIMEOUT + 1
                    if seconds_ago <= HEARTBEAT_TIMEOUT:
                        connected_trains.append(train_id)
                        print(f"[QUEUE] Tren {train_id} conectado (heartbeat {seconds_ago:.1f}s)")
                    else:
                        print(f"[QUEUE] Tren {train_id} sin heartbeat reciente ({seconds_ago:.1f}s)")
                else:
                    print(f"[QUEUE] Tren {train_id} sin heartbeat")

            if connected_trains:
                print(f"[QUEUE] Trenes conectados: {connected_trains}")
            else:
                print("[QUEUE] Sin heartbeats recientes - se mantiene el orden actual")
            
            # Actualizar colas de TODAS las estaciones
            with station_queue_lock:
                for station_id in station_queues.keys():
                    # Si la cola está vacía (ej. recién creada), usar orden por defecto
                    if not station_queues[station_id]:
                        station_queues[station_id] = DEFAULT_QUEUE_ORDER.copy()
                        print(f"[QUEUE] Est.{station_id} inicializada → {station_queues[station_id]}")
                        continue

                    # Si hay trenes conectados, ajustar cola para reflejar los disponibles
                    if connected_trains:
                        current_set = set(station_queues[station_id])
                        active_set = set(connected_trains)

                        if current_set != active_set:
                            station_queues[station_id] = connected_trains.copy()
                            print(f"[QUEUE] Est.{station_id} actualizada → {station_queues[station_id]}")
            
            time.sleep(5)  # Revisar cada 5 segundos (menos frecuente)
            
        except Exception as e:
            print(f"[QUEUE] Error: {e}")
            time.sleep(5)


def main():
    print("\n" + "="*60)
    print("  SISTEMA DE ESTACIONES - METRO BOGOTÁ v4.0")
    print("="*60)
    print(f"Redis: {REDIS_HOST}:{REDIS_PORT}")
    print(f"Estaciones configuradas: {len(STATIONS)}")
    print("="*60 + "\n")
    
    # Verificar Redis
    try:
        redis_client.ping()
        print("[OK] Conexión a Redis exitosa\n")
    except Exception as e:
        print(f"[ERROR] No se puede conectar a Redis: {e}")
        return
    
    # Iniciar thread de actualización de cola de trenes
    queue_thread = Thread(target=update_train_queue, daemon=True)
    queue_thread.start()
    print("[QUEUE] Monitor de trenes activos iniciado\n")
    
    # Crear y arrancar threads de sensores
    threads = []
    for station_id, config in STATIONS.items():
        sensor = StationSensor(station_id, config)
        thread = Thread(target=sensor.run, daemon=True)
        thread.start()
        threads.append(thread)
    
    print("\n[SYSTEM] Sistema de estaciones activo")
    print("Presiona Ctrl+C para detener\n")
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n\n[SYSTEM] Deteniendo sistema...")
        GPIO.cleanup()
        print("[SYSTEM] GPIO limpiado - Adiós")


if __name__ == "__main__":
    main()
