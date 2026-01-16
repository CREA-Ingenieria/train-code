"""
Web Server v3.0 - Optimizado con Rate Limiting
Servidor de control central para el sistema de trenes

MEJORAS v3.0:
- WebSocket optimizado (3s en lugar de 2s)
- Rate limiting para prevenir spam de comandos
- Cache de estado para reducir queries a Redis
- Mejor manejo de desconexiones
- Usa nuevo sistema de colas con prioridades
"""
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
import redis
import json
import asyncio
import time
from datetime import datetime
import os
from dotenv import load_dotenv
from typing import List, Dict
from collections import defaultdict

load_dotenv()

app = FastAPI(title="Metro Bogotá Control System v3.0")

# Rate limiting: máximo comandos por tren por minuto
RATE_LIMIT_COMMANDS_PER_MINUTE = 30
command_timestamps = defaultdict(list)  # {train_id: [timestamp1, timestamp2, ...]}

# Servir archivos estáticos (CSS, JS, fuentes)
app.mount("/static", StaticFiles(directory="frontend/static"), name="static")

# CORS para desarrollo
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configuración Redis
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))


redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    db=0,
    decode_responses=True
)

# Manager para WebSocket
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        print(f"[WS] Cliente conectado. Total: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)
        print(f"[WS] Cliente desconectado. Total: {len(self.active_connections)}")

    async def broadcast(self, message: dict):
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except:
                pass

manager = ConnectionManager()

# Suscriptor Redis para eventos de estaciones
pubsub = redis_client.pubsub()
pubsub.subscribe('stations:events')

async def station_events_listener():
    """Escucha eventos de estaciones y los broadcastea via WebSocket"""
    def get_message():
        message = pubsub.get_message()
        return message
    
    while True:
        message = await asyncio.to_thread(get_message)
        if message and message['type'] == 'message':
            try:
                event_data = json.loads(message['data'])
                await manager.broadcast({
                    "type": "station_event",
                    "data": event_data
                })
            except Exception as e:
                print(f"[PUBSUB] Error: {e}")
        await asyncio.sleep(0.1)


# ============== API ENDPOINTS ==============

@app.get("/")
async def root():
    """Sirve el frontend HTML"""
    with open("frontend/index.html", "r", encoding="utf-8") as f:
        html_content = f.read()
    return HTMLResponse(content=html_content)


@app.get("/api/trains/active")
async def get_active_trains():
    """Obtiene lista de trenes activos (con heartbeat reciente)"""
    try:
        pattern = "tren:*:heartbeat"
        keys = redis_client.keys(pattern)
        
        active_trains = []
        current_time = int(time.time())
        
        for key in keys:
            train_id = int(key.split(':')[1])
            last_heartbeat = redis_client.get(key)
            
            if last_heartbeat:
                last_heartbeat = int(last_heartbeat)
                seconds_ago = current_time - last_heartbeat
                
                if seconds_ago < 15:  # Activo en últimos 15 segundos
                    state = get_train_state_sync(train_id)
                    # FIXED: Incluir tren incluso si no tiene estado aún
                    if not state:
                        state = {
                            "direction": "STOP",
                            "speed": 0,
                            "motors_running": False,
                            "wifi_rssi": 0,
                            "uptime": 0,
                            "timestamp": "",
                            "animation_running": False
                        }
                    active_trains.append({
                        "train_id": train_id,
                        "last_heartbeat": last_heartbeat,
                        "seconds_ago": seconds_ago,
                        "state": state
                    })
        
        return {"success": True, "trains": active_trains}
    
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.get("/api/train/{train_id}/state")
async def get_train_state(train_id: int):
    """Obtiene estado actual de un tren"""
    state = get_train_state_sync(train_id)
    
    if state:
        return {"success": True, "train_id": train_id, "state": state}
    else:
        return {"success": False, "error": "train_not_found"}


def check_rate_limit(train_id: int) -> bool:
    """
    Verifica si el tren está dentro del rate limit.
    Retorna True si puede proceder, False si está limitado.
    """
    current_time = time.time()
    
    # Limpiar timestamps viejos (más de 60 segundos)
    command_timestamps[train_id] = [
        ts for ts in command_timestamps[train_id] 
        if current_time - ts < 60
    ]
    
    # Verificar si excede el límite
    if len(command_timestamps[train_id]) >= RATE_LIMIT_COMMANDS_PER_MINUTE:
        return False
    
    # Agregar timestamp actual
    command_timestamps[train_id].append(current_time)
    return True


@app.post("/api/train/{train_id}/command")
async def send_command(train_id: int, command_data: dict):
    """
    Envía un comando a un tren usando el nuevo sistema de prioridades.
    Body: {"comando": "move_forward", "parametros": {"velocidad_objetivo": 80}}
    
    MEJORADO: Usa rate limiting y el sistema de colas con prioridades.
    """
    try:
        comando = command_data.get("comando")
        parametros = command_data.get("parametros", {})
        
        if not comando:
            return {"success": False, "error": "missing_command"}
        
        # Rate limiting (excepto para comandos STOP que son críticos)
        if comando != "stop" and not check_rate_limit(train_id):
            raise HTTPException(
                status_code=429, 
                detail="Rate limit exceeded. Max 30 commands per minute per train."
            )
        
        # Usar el nuevo sistema de colas con prioridades del proxy
        # El proxy determina automáticamente la prioridad según el comando
        message = {
            "comando": comando,
            "parametros": parametros,
            "timestamp": time.time()
        }
        
        # Determinar prioridad (stop va a priority, resto a normal)
        if comando in ["stop", "emergency_stop"]:
            channel = f"tren/{train_id}/cmd/priority"
        else:
            channel = f"tren/{train_id}/cmd/normal"
        
        redis_client.lpush(channel, json.dumps(message))
        
        # Limitar tamaño de cola
        redis_client.ltrim(channel, 0, 99)
        
        # Broadcast a WebSocket
        await manager.broadcast({
            "type": "command_sent",
            "train_id": train_id,
            "comando": comando
        })
        
        return {"success": True, "train_id": train_id, "comando": comando}
    
    except HTTPException:
        raise
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.get("/api/stations/status")
async def get_stations_status():
    """Obtiene estado de todas las estaciones"""
    try:
        stations = []
        for i in [1, 2, 3]:
            state = redis_client.hgetall(f"station:{i}:state")
            if state:
                stations.append({
                    "station_id": i,
                    "train_present": state.get("train_present") == "1",
                    "distance": float(state.get("distance", 0)),
                    "last_update": float(state.get("last_update", 0))
                })
        return {"success": True, "stations": stations}
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.post("/api/train/{train_id}/auto_mode")
async def toggle_auto_mode(train_id: int, mode_data: dict):
    """
    Activa/desactiva modo automático
    Body: {"enabled": true/false}
    """
    try:
        enabled = mode_data.get("enabled", False)
        
        # Guardar estado de modo automático en Redis
        redis_client.hset(
            f"tren:{train_id}:auto",
            mapping={"enabled": "1" if enabled else "0"}
        )
        
        return {"success": True, "train_id": train_id, "auto_mode": enabled}
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.post("/api/train/{train_id}/station_config")
async def set_station_config(train_id: int, config_data: dict):
    """
    Configura tiempos de parada en estaciones para un tren
    Body: {"station_1_time": 5, "station_2_time": 5}
    """
    try:
        station_1_time = config_data.get("station_1_time", 5)
        station_2_time = config_data.get("station_2_time", 5)
        
        # Guardar en Redis
        redis_client.hset(
            f"tren:{train_id}:station_config",
            mapping={
                "station_1_time": str(station_1_time),
                "station_2_time": str(station_2_time)
            }
        )
        
        return {
            "success": True, 
            "train_id": train_id, 
            "config": {
                "station_1_time": station_1_time,
                "station_2_time": station_2_time
            }
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.get("/api/train/{train_id}/station_config")
async def get_station_config(train_id: int):
    """Obtiene configuración de tiempos de parada de un tren"""
    try:
        config = redis_client.hgetall(f"tren:{train_id}:station_config")
        
        if not config:
            # Valores por defecto
            return {
                "success": True,
                "train_id": train_id,
                "config": {
                    "station_1_time": 5,
                    "station_2_time": 5
                }
            }
        
        return {
            "success": True,
            "train_id": train_id,
            "config": {
                "station_1_time": int(config.get("station_1_time", 5)),
                "station_2_time": int(config.get("station_2_time", 5))
            }
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.get("/api/system/config")
async def get_system_config():
    """Obtiene configuración global del sistema"""
    try:
        config = redis_client.hgetall("system:config")
        
        # Valores por defecto
        result = {
            "train1_reached_station1": config.get("train1_reached_station1") == "1" if config else False,
            "max_speed_train1": int(config.get("max_speed_train1", 100)) if config else 100
        }
        
        return {"success": True, "config": result}
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.post("/api/system/config")
async def set_system_config(config_data: dict):
    """Actualiza configuración global del sistema"""
    try:
        updates = {}
        
        if "train1_reached_station1" in config_data:
            updates["train1_reached_station1"] = "1" if config_data["train1_reached_station1"] else "0"
        
        if "max_speed_train1" in config_data:
            updates["max_speed_train1"] = str(config_data["max_speed_train1"])
        
        if updates:
            redis_client.hset("system:config", mapping=updates)
        
        return {"success": True, "config": config_data}
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.get("/api/train/{train_id}/queue")
async def get_queue_length(train_id: int):
    """Obtiene cantidad de comandos pendientes en ambas colas"""
    try:
        priority_queue = f"tren/{train_id}/cmd/priority"
        normal_queue = f"tren/{train_id}/cmd/normal"
        
        priority_length = redis_client.llen(priority_queue) or 0
        normal_length = redis_client.llen(normal_queue) or 0
        
        return {
            "success": True, 
            "train_id": train_id, 
            "queue_length": {
                "priority": priority_length,
                "normal": normal_length,
                "total": priority_length + normal_length
            }
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.delete("/api/train/{train_id}/queue")
async def clear_queue(train_id: int, queue_type: str = "all"):
    """
    Limpia la cola de comandos (emergencia).
    queue_type: 'all', 'priority', o 'normal'
    """
    try:
        if queue_type == "all":
            redis_client.delete(f"tren/{train_id}/cmd/priority")
            redis_client.delete(f"tren/{train_id}/cmd/normal")
        elif queue_type == "priority":
            redis_client.delete(f"tren/{train_id}/cmd/priority")
        elif queue_type == "normal":
            redis_client.delete(f"tren/{train_id}/cmd/normal")
        else:
            return {"success": False, "error": "invalid_queue_type"}
        
        return {"success": True, "train_id": train_id, "cleared": queue_type}
    except Exception as e:
        return {"success": False, "error": str(e)}


# ============== NUEVOS ENDPOINTS: CONTROL GENERAL DEL SISTEMA ==============

@app.post("/api/system/iniciar-auto-general")
async def iniciar_auto_general(config_data: dict):
    """
    Inicia el modo automático para todos los trenes con configuración personalizada
    Body: {
        "tren1": {"velocidad": 75, "tiempo_est1": 5, "tiempo_est3": 5, "tiempo_est2": 5},
        "tren2": {"velocidad": 75, "tiempo_est1": 5, "tiempo_est3": 5, "tiempo_est2": 5}
    }
    """
    try:
        tren1_config = config_data.get("tren1", {})
        tren2_config = config_data.get("tren2", {})
        
        # Configurar tren 1
        if tren1_config:
            # Guardar velocidad crucero
            redis_client.hset("tren:1:config", "cruise_speed", str(tren1_config.get("velocidad", 75)))
            
            # Guardar tiempos de estación
            redis_client.hset("tren:1:station_config", mapping={
                "station_1_time": str(tren1_config.get("tiempo_est1", 5)),
                "station_3_time": str(tren1_config.get("tiempo_est3", 5)),
                "station_2_time": str(tren1_config.get("tiempo_est2", 5))
            })
            
            # Activar modo automático
            redis_client.hset("tren:1:auto", "enabled", "1")
        
        # Configurar tren 2
        if tren2_config:
            # Guardar velocidad crucero
            redis_client.hset("tren:2:config", "cruise_speed", str(tren2_config.get("velocidad", 75)))
            
            # Guardar tiempos de estación
            redis_client.hset("tren:2:station_config", mapping={
                "station_1_time": str(tren2_config.get("tiempo_est1", 5)),
                "station_3_time": str(tren2_config.get("tiempo_est3", 5)),
                "station_2_time": str(tren2_config.get("tiempo_est2", 5))
            })
            
            # Activar modo automático
            redis_client.hset("tren:2:auto", "enabled", "1")
        
        return {
            "success": True, 
            "message": "Sistema automático iniciado para todos los trenes configurados"
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.post("/api/system/parada-general")
async def parada_general():
    """
    Detiene todos los trenes y desactiva el modo automático
    """
    try:
        # Buscar todos los trenes activos
        pattern = "tren:*:heartbeat"
        keys = redis_client.keys(pattern)
        
        trenes_detenidos = []
        
        for key in keys:
            train_id = int(key.split(':')[1])
            
            # Desactivar modo automático
            redis_client.hset(f"tren:{train_id}:auto", "enabled", "0")
            
            # Enviar comando de stop prioritario
            stop_command = {
                "comando": "stop",
                "parametros": {},
                "timestamp": time.time()
            }
            redis_client.lpush(f"tren/{train_id}/cmd/priority", json.dumps(stop_command))
            
            trenes_detenidos.append(train_id)
        
        return {
            "success": True,
            "message": f"Detenidos {len(trenes_detenidos)} trenes",
            "trenes": trenes_detenidos
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.post("/api/system/reset")
async def reset_sistema():
    """
    Resetea el sistema completo: detiene trenes, limpia colas y configuración
    Resetea las colas de las estaciones para que vuelvan a esperar al tren 1
    """
    try:
        # Buscar todos los trenes
        pattern = "tren:*:heartbeat"
        keys = redis_client.keys(pattern)
        
        for key in keys:
            train_id = int(key.split(':')[1])
            
            # Desactivar modo automático
            redis_client.hset(f"tren:{train_id}:auto", "enabled", "0")
            
            # Enviar comando de stop
            stop_command = {
                "comando": "stop",
                "parametros": {},
                "timestamp": time.time()
            }
            redis_client.lpush(f"tren/{train_id}/cmd/priority", json.dumps(stop_command))
            
            # Limpiar colas
            redis_client.delete(f"tren/{train_id}/cmd/priority")
            redis_client.delete(f"tren/{train_id}/cmd/normal")
            
            # Limpiar configuración de estaciones
            redis_client.delete(f"tren:{train_id}:station_config")
            redis_client.delete(f"tren:{train_id}:config")
        
        # Resetear configuración del sistema
        redis_client.delete("system:config")
        
        # NUEVO: Resetear las colas de las estaciones
        # Esto hace que ambas estaciones vuelvan a esperar al tren 1 primero
        redis_client.set("system:reset_station_queues", "1")
        # Esta flag será leída por station_manager para resetear sus colas
        
        return {
            "success": True,
            "message": "Sistema reseteado completamente - Las estaciones volverán a esperar al tren 1"
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


# ============== WEBSOCKET ==============

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """
    WebSocket para actualizaciones en tiempo real - OPTIMIZADO
    Envía estado de todos los trenes activos cada 3 segundos (reducido de 2s)
    """
    await manager.connect(websocket)
    
    try:
        while True:
            # Obtener estado de todos los trenes activos
            active_trains = await get_all_trains_state()
            
            # Enviar a cliente
            await websocket.send_json({
                "type": "state_update",
                "timestamp": datetime.now().isoformat(),
                "trains": active_trains
            })
            
            # Esperar 3 segundos antes de próxima actualización (OPTIMIZADO)
            # Reduce tráfico de red y carga de CPU
            await asyncio.sleep(3)
    
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception as e:
        print(f"[WS] Error: {e}")
        manager.disconnect(websocket)


# ============== FUNCIONES AUXILIARES ==============

def get_train_state_sync(train_id: int) -> Dict:
    """Obtiene estado de un tren desde Redis (síncrono)"""
    key = f"tren:{train_id}:state"
    state = redis_client.hgetall(key)
    
    if not state:
        return None
    
    # Convertir strings a tipos apropiados
    return {
        "direction": state.get("direction", "STOP"),
        "speed": int(state.get("speed", 0)),
        "motors_running": state.get("motors_running") == "1",
        "wifi_rssi": int(state.get("wifi_rssi", 0)),
        "uptime": int(state.get("uptime", 0)),
        "timestamp": state.get("timestamp", ""),
        "animation_running": state.get("animation_running") == "1"
    }


async def get_all_trains_state() -> List[Dict]:
    """Obtiene estado de todos los trenes activos"""
    pattern = "tren:*:heartbeat"
    keys = redis_client.keys(pattern)
    
    trains = []
    current_time = int(time.time())
    
    for key in keys:
        train_id = int(key.split(':')[1])
        last_heartbeat = redis_client.get(key)
        
        if last_heartbeat:
            last_heartbeat = int(last_heartbeat)
            seconds_ago = current_time - last_heartbeat
            
            if seconds_ago < 15:
                state = get_train_state_sync(train_id)
                # FIXED: Incluir tren incluso si no tiene estado aún
                if not state:
                    state = {
                        "direction": "STOP",
                        "speed": 0,
                        "motors_running": False,
                        "wifi_rssi": 0,
                        "uptime": 0,
                        "timestamp": "",
                        "animation_running": False
                    }
                trains.append({
                    "train_id": train_id,
                    "state": state,
                    "last_seen": seconds_ago
                })
    
    return trains


# ============== STARTUP ==============

@app.on_event("startup")
async def startup_event():
    print("\n" + "="*60)
    print("  METRO BOGOTÁ - WEB SERVER")
    print("="*60)
    print(f"Redis: {REDIS_HOST}:{REDIS_PORT}")
    print("Servidor web iniciado en: http://0.0.0.0:8000")
    print("WebSocket endpoint: ws://0.0.0.0:8000/ws")
    print("="*60 + "\n")
    
    # Verificar conexión Redis
    try:
        redis_client.ping()
        print("[OK] Conexión a Redis exitosa")
    except Exception as e:
        print(f"[ERROR] No se puede conectar a Redis: {e}")
    
    # Iniciar listener de eventos de estaciones
    asyncio.create_task(station_events_listener())


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

