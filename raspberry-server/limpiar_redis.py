#!/usr/bin/env python3
"""
Script para limpiar datos residuales de Redis
Uso: python3 limpiar_redis.py
"""

import redis
import sys

# Conexión a Redis
REDIS_HOST = "localhost"
REDIS_PORT = 6379

try:
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
    redis_client.ping()
    print("✓ Conectado a Redis")
except Exception as e:
    print(f"✗ Error conectando a Redis: {e}")
    sys.exit(1)

print("\n" + "="*60)
print("  LIMPIEZA DE REDIS - Sistema de Trenes")
print("="*60 + "\n")

# Mostrar keys actuales antes de limpiar
print("📋 Keys actuales en Redis:\n")

patterns_to_check = [
    "tren:*:auto",
    "tren:*:heartbeat",
    "tren:*:config",
    "tren:*:station_config",
    "tren:*:state",
    "station:*:state",
    "system:*"
]

total_keys = 0
for pattern in patterns_to_check:
    keys = redis_client.keys(pattern)
    if keys:
        print(f"  {pattern}:")
        for key in sorted(keys):
            if "auto" in key:
                enabled = redis_client.hget(key, "enabled")
                print(f"    - {key} (enabled={enabled})")
            elif "heartbeat" in key:
                hb = redis_client.get(key)
                print(f"    - {key} (value={hb})")
            else:
                print(f"    - {key}")
            total_keys += 1

print(f"\n📊 Total de keys: {total_keys}")

if total_keys == 0:
    print("\n✓ Redis ya está limpio")
    sys.exit(0)

# Preguntar confirmación
print("\n" + "="*60)
respuesta = input("¿Desea limpiar TODAS estas keys? (si/no): ").lower().strip()

if respuesta not in ['si', 'sí', 's', 'yes', 'y']:
    print("Operación cancelada")
    sys.exit(0)

# Limpiar todas las keys
print("\n🧹 Limpiando Redis...\n")

keys_deleted = 0
for pattern in patterns_to_check:
    keys = redis_client.keys(pattern)
    for key in keys:
        redis_client.delete(key)
        print(f"  ✓ Eliminada: {key}")
        keys_deleted += 1

# Limpiar flag de reset
redis_client.delete("system:reset_station_queues")

print(f"\n✅ Limpieza completada: {keys_deleted} keys eliminadas")
print("\n📌 Acciones recomendadas:")
print("  1. Reiniciar station_manager.py")
print("  2. Reiniciar auto_controller.py (si está corriendo)")
print("  3. Recargar la página web (Ctrl+F5)")
print("  4. Volver a configurar el sistema desde el frontend")

print("\n" + "="*60)


