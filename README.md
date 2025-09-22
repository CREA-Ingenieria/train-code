# train-code

# Crear un ambiente virtual 
## Ubuntu/Debian - Bash
### 1. (Opcional) Instalar el paquete venv si no está presente 
```bash
sudo apt-get update
sudo apt-get install python3-venv -y
```

### 2. Navegar a la carpeta de tu proyecto
```bash
cd ruta_proyecto
```

### 3. Crear el ambiente virtual (se creará una carpeta 'venv')
```bash
python3 -m venv venv
```

### 4. Activar el ambiente virtual. Nota el uso de 'source'.
```bash
source venv/bin/activate
```

### 5. Para desactivar el ambiente 
```bash
deactivate
```

## Windows - Powershell
### 1. Navegar a la carpeta de tu proyecto
```bash
cd ruta_proyecto
```

### 2. Crear el ambiente virtual (se creará una carpeta 'venv')
```bash
python -m venv venv
```

### 3. (Opcional) Si la activación falla, permite la ejecución de scripts para esta sesión
```bash
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope Process
```

### 4. Activar el ambiente virtual
```bash
.\venv\Scripts\Activate
```

### 5. Para desactivar el ambiente 
```bash
deactivate
```

# Desplegar Redis
## Docker
```bash
docker run -d -p 6379:6379 --name redis-server redis
```