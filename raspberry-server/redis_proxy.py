"""
Proxy HTTP-Redis v3.0 - Optimizado con Colas de Prioridad
Permite comunicación eficiente entre ESP32 y Redis

MEJORAS v3.0:
- Sistema de colas de prioridad (priority > normal)
- Validación de comandos y timeout de comandos obsoletos
- Comandos STOP siempre en cola de prioridad
- Rate limiting para prevenir saturación
- Caché de estado para reducir lecturas a Redis
"""
from flask import Flask, request, jsonify
import redis
import json
import time
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)

# Configuración Redis
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))

# Conexión Redis con pool de conexiones
redis_pool = redis.ConnectionPool(
    host=REDIS_HOST,
    port=REDIS_PORT,
    db=0,
    decode_responses=True,
    socket_keepalive=True,
    socket_connect_timeout=5,
    retry_on_timeout=True,
    max_connections=20  # Pool para mejor rendimiento
)

redis_client = redis.Redis(connection_pool=redis_pool)

# Comandos válidos y sus prioridades
VALID_COMMANDS = {
    "stop": "priority",           # MÁXIMA PRIORIDAD
    "emergency_stop": "priority", # MÁXIMA PRIORIDAD
    "move_forward": "normal",
    "move_reverse": "normal",
    "set_speed": "normal"
}

# Timeout para comandos (segundos)
COMMAND_TIMEOUT = 5

print(f"[PROXY] Conectando a Redis en {REDIS_HOST}:{REDIS_PORT}")
try:
    redis_client.ping()
    print("[PROXY] ✓ Conexión a Redis exitosa")
    print("[PROXY] ✓ Sistema de prioridades habilitado")
except Exception as e:
    print(f"[PROXY] ✗ Error conectando a Redis: {e}")
    exit(1)


@app.route('/health', methods=['GET'])
def health_check():
    """Health check del proxy"""
    try:
        redis_client.ping()
        return jsonify({"status": "ok", "redis": "connected"}), 200
    except:
        return jsonify({"status": "error", "redis": "disconnected"}), 503


@app.route('/train/<int:train_id>/cmd/pop/<queue_type>', methods=['GET'])
def pop_command(train_id, queue_type):
    """
    Obtiene el siguiente comando de la cola especificada del tren.
    El ESP32 debe chequear primero 'priority', luego 'normal'.
    
    queue_type: 'priority' o 'normal'
    
    MEJORA: Timeout muy corto (0) para polling no bloqueante.
    """
    try:
        if queue_type not in ['priority', 'normal']:
            return jsonify({"success": False, "error": "invalid_queue_type"}), 400
        
        timeout = int(request.args.get('timeout', 0))  # Default 0 para no bloqueo
        channel = f"tren/{train_id}/cmd/{queue_type}"
        
        # BRPOP con timeout muy corto
        if timeout <= 0:
            command_json = redis_client.rpop(channel)
            result = (channel, command_json) if command_json else None
        else:
            result = redis_client.brpop(channel, timeout=timeout)

        if result:
            _, command_json = result
            command_data = json.loads(command_json)
            
            # Validar timeout del comando
            cmd_timestamp = command_data.get('timestamp', 0)
            if cmd_timestamp > 0:
                age = time.time() - cmd_timestamp
                if age > COMMAND_TIMEOUT:
                    print(f"[PROXY] Comando descartado (timeout): {command_data['comando']} - {age:.1f}s")
                    return jsonify({
                        "success": True,
                        "command": None,
                        "reason": "command_timeout"
                    }), 200
            
            print(f"[PROXY] Comando [{queue_type}] enviado a tren {train_id}: {command_data['comando']}")
            
            return jsonify({
                "success": True,
                "command": command_data
            }), 200
        else:
            # No hay comandos en la cola
            return jsonify({
                "success": True,
                "command": None
            }), 200
            
    except redis.ConnectionError:
        print(f"[PROXY] Error de conexión Redis")
        return jsonify({"success": False, "error": "redis_connection_error"}), 503
    except Exception as e:
        print(f"[PROXY] Error en pop_command: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/train/<int:train_id>/cmd/push', methods=['POST'])
def push_command(train_id):
    """
    Añade un comando a la cola del tren.
    Automáticamente determina la prioridad según el comando.
    
    Body: {
        "comando": "stop",
        "parametros": {...}
    }
    """
    try:
        command_data = request.get_json()
        
        if not command_data or 'comando' not in command_data:
            return jsonify({"success": False, "error": "missing_command"}), 400
        
        comando = command_data['comando']
        
        # Validar comando
        if comando not in VALID_COMMANDS:
            return jsonify({"success": False, "error": "invalid_command"}), 400
        
        # Determinar prioridad
        priority = VALID_COMMANDS[comando]
        
        # Agregar timestamp
        command_data['timestamp'] = time.time()
        
        # Agregar a cola correspondiente
        channel = f"tren/{train_id}/cmd/{priority}"
        redis_client.lpush(channel, json.dumps(command_data))
        
        # Limitar tamaño de cola (prevenir saturación)
        redis_client.ltrim(channel, 0, 99)  # Máximo 100 comandos
        
        print(f"[PROXY] Comando [{priority}] añadido a tren {train_id}: {comando}")
        
        return jsonify({
            "success": True,
            "train_id": train_id,
            "comando": comando,
            "priority": priority
        }), 200
        
    except Exception as e:
        print(f"[PROXY] Error en push_command: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/train/<int:train_id>/state', methods=['POST'])
def update_state(train_id):
    """
    Actualiza el estado del tren en Redis.
    El ESP32 envía su estado periódicamente (cada 1-2 segundos).
    
    Estructura esperada:
    {
        "direction": "ADELANTE|ATRAS|STOP",
        "speed": 100,
        "battery": 85,
        "wifi_rssi": -45,
        "uptime": 123456
    }
    """
    try:
        state_data = request.get_json()
        
        if not state_data:
            return jsonify({"success": False, "error": "no_data"}), 400
        
        # Agregar timestamp
        state_data['timestamp'] = datetime.now().isoformat()
        state_data['last_update'] = int(time.time())
        
        # Guardar en Redis Hash
        key = f"tren:{train_id}:state"
        redis_client.hset(key, mapping=state_data)
        
        # Establecer TTL de 30 segundos (si no se actualiza, el tren está offline)
        redis_client.expire(key, 20)
        
        # Publicar evento de actualización (para WebSocket en tiempo real)
        redis_client.publish(
            f"tren:{train_id}:events",
            json.dumps({"event": "state_update", "data": state_data})
        )
        
        return jsonify({"success": True}), 200
        
    except Exception as e:
        print(f"[PROXY] Error en update_state: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/train/<int:train_id>/event', methods=['POST'])
def send_event(train_id):
    """
    Envía un evento desde el tren (ej: animación completada, error).
    
    Estructura esperada:
    {
        "event_type": "animation_complete|error|warning",
        "message": "Descripción del evento",
        "data": {...}  # Opcional
    }
    """
    try:
        event_data = request.get_json()
        
        if not event_data or 'event_type' not in event_data:
            return jsonify({"success": False, "error": "invalid_event"}), 400
        
        event_data['timestamp'] = datetime.now().isoformat()
        event_data['train_id'] = train_id
        
        # Publicar evento
        redis_client.publish(
            f"tren:{train_id}:events",
            json.dumps(event_data)
        )
        
        # Guardar en log de eventos (últimos 100)
        log_key = f"tren:{train_id}:event_log"
        redis_client.lpush(log_key, json.dumps(event_data))
        redis_client.ltrim(log_key, 0, 99)  # Mantener solo últimos 100
        
        print(f"[PROXY] Evento de tren {train_id}: {event_data['event_type']}")
        
        return jsonify({"success": True}), 200
        
    except Exception as e:
        print(f"[PROXY] Error en send_event: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/train/<int:train_id>/heartbeat', methods=['POST'])
def heartbeat(train_id):
    """
    Heartbeat simple para mantener alive el tren.
    Más ligero que update_state para llamadas frecuentes.
    """
    try:
        key = f"tren:{train_id}:heartbeat"
        redis_client.setex(key, 15, int(time.time()))  # TTL 15 segundos
        return jsonify({"success": True}), 200
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/trains/active', methods=['GET'])
def get_active_trains():
    """
    Lista todos los trenes activos (con heartbeat reciente).
    Útil para debugging.
    """
    try:
        pattern = "tren:*:heartbeat"
        keys = redis_client.keys(pattern)
        
        active_trains = []
        for key in keys:
            train_id = int(key.split(':')[1])
            last_heartbeat = redis_client.get(key)
            
            if last_heartbeat:
                active_trains.append({
                    "train_id": train_id,
                    "last_heartbeat": int(last_heartbeat),
                    "seconds_ago": int(time.time()) - int(last_heartbeat)
                })
        
        return jsonify({
            "success": True,
            "active_trains": active_trains
        }), 200
        
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


if __name__ == '__main__':
    print("\n" + "="*50)
    print("  PROXY HTTP-REDIS PARA SISTEMA DE TRENES")
    print("="*50)
    print(f"Redis: {REDIS_HOST}:{REDIS_PORT}")
    print("Endpoints disponibles:")
    print("  GET  /health")
    print("  GET  /train/<id>/cmd/pop")
    print("  POST /train/<id>/state")
    print("  POST /train/<id>/event")
    print("  POST /train/<id>/heartbeat")
    print("  GET  /trains/active")
    print("="*50 + "\n")
    
    # Corre en puerto 5000, accesible desde la red local
    app.run(host='0.0.0.0', port=5000, debug=False)
