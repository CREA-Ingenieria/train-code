/*
 * Metro Bogotá - ESP32 Train Worker v3.0
 * Código optimizado con sistema de prioridades y watchdog
 * 
 * MEJORAS v3.0:
 * - Watchdog timer para recuperación automática
 * - Colas de prioridad (STOP > Manual > Auto)
 * - Intervalos optimizados para reducir latencia
 * - Reconexión WiFi robusta con exponential backoff
 * - Protección contra disparos aleatorios de motores
 * - Timeout de comandos obsoletos
 */

#include <WiFi.h>
#include <HTTPClient.h>
#include <ArduinoJson.h>
#include <Wire.h>
#include <Adafruit_GFX.h>
#include <Adafruit_SSD1306.h>
#include <esp_task_wdt.h>

// ============== CONFIGURACIÓN (CAMBIAR SEGÚN TU SETUP) ==============

// WiFi (Raspberry Pi Hotspot)
const char* WIFI_SSID = "RaspberryPi_Hotspot";
const char* WIFI_PASSWORD = "metro123";

// Redis Proxy en Raspberry Pi
const char* PROXY_HOST = "metro01.local";
const int PROXY_PORT = 5000;

// ID único del tren (CAMBIAR para cada ESP32: 1, 2, 3...)
const int TRAIN_ID = 1;

// Pines L298N
const int IN1 = 12;  // Motor A
const int IN2 = 13;
const int IN3 = 2;  // Motor B
const int IN4 = 4;

// Configuración OLED
#define SCREEN_WIDTH 128
#define SCREEN_HEIGHT 64
#define OLED_RESET -1
#define SCREEN_ADDRESS 0x3C
#define SDA_PIN 16
#define SCL_PIN 3

// PWM
const int PWM_FREQ = 1000;
const int PWM_RESOLUTION = 8;
const int MAX_SPEED = 168;  // Límite 3.3V: 255 * (3.3/5.0) ≈ 168

// Watchdog Timer
const int WDT_TIMEOUT = 10;  // 10 segundos - reinicio si no se resetea

// ============== INTERVALOS OPTIMIZADOS ==============
// CRÍTICO: Intervalos optimizados para MÁXIMA RESPUESTA

const unsigned long COMMAND_POLL_INTERVAL = 100;    // 100ms - ULTRA RESPONSIVO
const unsigned long STATE_UPDATE_INTERVAL = 3000;   // 3 segundos - reducir tráfico
const unsigned long HEARTBEAT_INTERVAL = 8000;      // 8 segundos - suficiente para keepalive
const unsigned long WIFI_CHECK_INTERVAL = 5000;     // 5 segundos - verificar WiFi menos frecuente

// ============== VARIABLES GLOBALES ==============

Adafruit_SSD1306 display(SCREEN_WIDTH, SCREEN_HEIGHT, &Wire, OLED_RESET);
bool oledReady = false;

struct {
  String direction;
  int speed;
  bool motorsRunning;
  unsigned long uptime;
  int wifiRetries;  // Contador de reintentos de WiFi
} state;

unsigned long lastCommandPoll = 0;
unsigned long lastStateUpdate = 0;
unsigned long lastHeartbeat = 0;
unsigned long lastWifiCheck = 0;
unsigned long lastMotorCommand = 0;  // Para timeout de comandos

bool systemReady = false;
bool wifiConnected = false;

// ============== SETUP ==============

void setup() {
  Serial.begin(115200);
  delay(2000);
  
  Serial.println("\n========================================");
  Serial.println("  METRO BOGOTÁ - TRAIN WORKER v3.0");
  Serial.printf("  Train ID: %d\n", TRAIN_ID);
  Serial.println("  [OPTIMIZADO - BAJA LATENCIA]");
  Serial.println("========================================\n");
  
  // CRÍTICO: Configurar watchdog timer PRIMERO
  Serial.println("[WDT] Configurando watchdog timer...");
  
  // Configuración compatible con ESP32 Arduino >= 3.0
  esp_task_wdt_config_t wdt_config = {
    .timeout_ms = WDT_TIMEOUT * 1000,  // Convertir segundos a milisegundos
    .idle_core_mask = 0,                // No monitorear idle tasks
    .trigger_panic = true               // Reiniciar si timeout
  };
  
  esp_task_wdt_init(&wdt_config);
  esp_task_wdt_add(NULL);  // Agregar tarea actual al watchdog
  Serial.println("[WDT] Watchdog activado - timeout 10s");
  
  // Inicializar estado ANTES de configurar pines
  state.direction = "STOP";
  state.speed = 0;
  state.motorsRunning = false;
  state.uptime = 0;
  state.wifiRetries = 0;
  
  Serial.println("[MOTOR] Inicializando control de motores...");
  
  // PASO 1: Configurar pines como OUTPUT y asegurar LOW
  pinMode(IN1, OUTPUT);
  pinMode(IN2, OUTPUT);
  pinMode(IN3, OUTPUT);
  pinMode(IN4, OUTPUT);
  
  // PASO 2: CRÍTICO - Forzar todos los pines a LOW INMEDIATAMENTE
  // Esto previene cualquier pulso durante la inicialización
  digitalWrite(IN1, LOW);
  digitalWrite(IN2, LOW);
  digitalWrite(IN3, LOW);
  digitalWrite(IN4, LOW);
  
  Serial.println("[MOTOR] Pines configurados en LOW");
  delay(200);  // Esperar estabilización
  
  // PASO 3: Configurar PWM comenzando con duty cycle = 0
  // IMPORTANTE: ledcAttach puede causar un pulso breve, por eso aseguramos LOW primero
  ledcAttach(IN1, PWM_FREQ, PWM_RESOLUTION);
  ledcWrite(IN1, 0);  // Inmediatamente escribir 0
  delay(50);
  
  ledcAttach(IN2, PWM_FREQ, PWM_RESOLUTION);
  ledcWrite(IN2, 0);
  delay(50);
  
  ledcAttach(IN3, PWM_FREQ, PWM_RESOLUTION);
  ledcWrite(IN3, 0);
  delay(50);
  
  ledcAttach(IN4, PWM_FREQ, PWM_RESOLUTION);
  ledcWrite(IN4, 0);
  delay(50);
  
  Serial.println("[MOTOR] PWM configurado en todos los canales");
  
  // PASO 4: Aseguramiento múltiple con delays mayores
  stopMotors();
  delay(200);
  stopMotors();
  delay(200);
  stopMotors();
  
  Serial.println("[MOTOR] ✓ Motores COMPLETAMENTE DETENIDOS y seguros");
  
  // Inicializar OLED (no crítico, puede fallar)
  initOLED();
  
  // Conectar WiFi con reintentos
  Serial.println("\n[WIFI] Conectando a red (motores bloqueados)...");
  wifiConnected = connectWiFi();
  if (!wifiConnected) {
    Serial.println("[WARN] WiFi no conectado inicialmente");
    Serial.println("[SISTEMA] Continuará intentando en background...");
    systemReady = false;  // NO marcar como listo hasta conectar
  } else {
    Serial.println("[WIFI] ✓ Conectado exitosamente");
    
    // Test de conexión al proxy (no crítico)
    if (testProxyConnection()) {
      Serial.println("[PROXY] ✓ Redis alcanzable");
      systemReady = true;  // Solo marcar listo si hay conexión completa
    } else {
      Serial.println("[PROXY] ⚠ No se pudo alcanzar Redis, reintentando...");
      systemReady = false;
    }
  }
  
  // Asegurar una vez más que motores están detenidos antes de marcar listo
  stopMotors();
  delay(100);
  
  esp_task_wdt_reset();  // Reset watchdog después de setup
  
  if (systemReady) {
    Serial.println("\n[OK] ✓✓✓ Sistema COMPLETAMENTE LISTO ✓✓✓");
    Serial.println("[OK] Motores desbloqueados - Esperando comandos\n");
  } else {
    Serial.println("\n[WAIT] Sistema en espera de conexión");
    Serial.println("[SAFE] Motores permanecen BLOQUEADOS hasta conectar\n");
  }
  
  updateOLEDStatus();
}

// ============== LOOP PRINCIPAL ==============

void loop() {
  // CRÍTICO: Resetear watchdog en cada iteración
  esp_task_wdt_reset();
  
  state.uptime = millis();
  
  // Verificar WiFi periódicamente (cada 5 segundos, no en cada loop)
  if (millis() - lastWifiCheck >= WIFI_CHECK_INTERVAL) {
    lastWifiCheck = millis();
    
    if (WiFi.status() != WL_CONNECTED) {
      Serial.println("[WiFi] Desconectado - Intentando reconectar...");
      systemReady = false;
      wifiConnected = false;
      
      // CRÍTICO: DETENER MOTORES INMEDIATAMENTE si se pierde WiFi
      stopMotors();
      stopMotors();  // Doble aseguramiento
      
      if (connectWiFiWithBackoff()) {
        systemReady = true;
        wifiConnected = true;
        state.wifiRetries = 0;
        Serial.println("[WiFi] Reconectado exitosamente");
      } else {
        state.wifiRetries++;
        Serial.printf("[WiFi] Reintento %d fallido\n", state.wifiRetries);
        
        // Si falla muchas veces, reiniciar ESP
        if (state.wifiRetries > 20) {
          Serial.println("[FATAL] Demasiados fallos de WiFi - Reiniciando...");
          delay(1000);
          ESP.restart();
        }
      }
    } else {
      wifiConnected = true;
      systemReady = true;
    }
  }
  
  // Solo ejecutar lógica si WiFi está conectado
  if (!systemReady || !wifiConnected) {
    delay(500);
    return;
  }
  
  // Timeout de comandos de motor - si lleva >30s sin comando, detener por seguridad
  if (state.motorsRunning && (millis() - lastMotorCommand > 30000)) {
    Serial.println("[SAFETY] Timeout de comando de motor - Deteniendo");
    stopMotors();
  }
  
  // Polling de comandos (cada 500ms - OPTIMIZADO)
  if (millis() - lastCommandPoll >= COMMAND_POLL_INTERVAL) {
    lastCommandPoll = millis();
    pollCommands();
  }
  
  // Actualizar estado (cada 5 segundos - REDUCIDO para menor tráfico)
  if (millis() - lastStateUpdate >= STATE_UPDATE_INTERVAL) {
    lastStateUpdate = millis();
    updateState();
  }
  
  // Heartbeat (cada 10 segundos - OPTIMIZADO)
  if (millis() - lastHeartbeat >= HEARTBEAT_INTERVAL) {
    lastHeartbeat = millis();
    sendHeartbeat();
  }
  
  delay(100);  // Delay razonable para no saturar CPU
}

// ============== WIFI CON EXPONENTIAL BACKOFF ==============

bool connectWiFi() {
  Serial.print("[WiFi] Conectando a: ");
  Serial.println(WIFI_SSID);
  
  WiFi.mode(WIFI_STA);
  WiFi.disconnect();
  delay(100);
  
  // Configuración para conexión más robusta
  WiFi.setAutoReconnect(true);
  WiFi.setMinSecurity(WIFI_AUTH_WPA_PSK);
  WiFi.begin(WIFI_SSID, WIFI_PASSWORD);
  
  int attempts = 0;
  while (WiFi.status() != WL_CONNECTED && attempts < 40) {
    delay(500);
    Serial.print(".");
    attempts++;
    
    // Reset watchdog durante conexión WiFi
    if (attempts % 5 == 0) {
      esp_task_wdt_reset();
    }
  }
  Serial.println();
  
  if (WiFi.status() == WL_CONNECTED) {
    Serial.println("[WiFi] Conectado exitosamente");
    Serial.print("[WiFi] IP: ");
    Serial.println(WiFi.localIP());
    Serial.print("[WiFi] RSSI: ");
    Serial.print(WiFi.RSSI());
    Serial.println(" dBm");
    return true;
  } else {
    Serial.println("[WiFi] Fallo al conectar");
    return false;
  }
}

bool connectWiFiWithBackoff() {
  // Exponential backoff para reconexiones
  int retryDelay = 1000;  // Empezar con 1 segundo
  
  for (int attempt = 0; attempt < 3; attempt++) {
    Serial.printf("[WiFi] Intento %d de reconexión...\n", attempt + 1);
    
    if (connectWiFi()) {
      return true;
    }
    
    // Esperar antes de reintentar (exponential backoff)
    Serial.printf("[WiFi] Esperando %dms antes de reintentar...\n", retryDelay);
    unsigned long startWait = millis();
    while (millis() - startWait < retryDelay) {
      esp_task_wdt_reset();  // Reset watchdog durante espera
      delay(100);
    }
    
    retryDelay *= 2;  // Duplicar tiempo de espera
  }
  
  return false;
}

// ============== PROXY REDIS ==============

bool testProxyConnection() {
  Serial.println("[PROXY] Verificando conexión...");
  
  HTTPClient http;
  String url = String("http://") + PROXY_HOST + ":" + PROXY_PORT + "/health";
  
  http.begin(url);
  http.setTimeout(5000);
  
  int httpCode = http.GET();
  http.end();
  
  if (httpCode == 200) {
    Serial.println("[PROXY] Conexión OK");
    return true;
  } else {
    Serial.printf("[PROXY] Error HTTP %d\n", httpCode);
    return false;
  }
}

void pollCommands() {
  // Primero chequear cola de ALTA PRIORIDAD (STOP, emergencias)
  pollCommandQueue("priority");
  
  // Luego cola normal
  pollCommandQueue("normal");
}

void pollCommandQueue(String queueType) {
  HTTPClient http;
  String url = String("http://") + PROXY_HOST + ":" + PROXY_PORT + 
               "/train/" + TRAIN_ID + "/cmd/pop/" + queueType + "?timeout=0";
  
  http.begin(url);
  http.setTimeout(800);  // Timeout ultra-corto para máxima respuesta
  
  int httpCode = http.GET();
  
  if (httpCode == 200) {
    String payload = http.getString();
    
    StaticJsonDocument<512> doc;
    DeserializationError error = deserializeJson(doc, payload);
    
    if (!error && doc["success"]) {
      if (!doc["command"].isNull()) {
        String comando = doc["command"]["comando"].as<String>();
        JsonObject params = doc["command"]["parametros"];
        unsigned long cmdTimestamp = doc["command"]["timestamp"] | 0;
        
        // CRÍTICO: Descartar comandos muy viejos (>5 segundos)
        if (cmdTimestamp > 0) {
          unsigned long cmdAge = millis() - (cmdTimestamp * 1000);
          if (cmdAge > 5000) {
            Serial.printf("[CMD] Descartado (muy viejo): %s - %lums\n", comando.c_str(), cmdAge);
            http.end();
            return;
          }
        }
        
        Serial.printf("[CMD-%s] Recibido: %s\n", queueType.c_str(), comando.c_str());
        executeCommand(comando, params);
      }
    }
  } else if (httpCode > 0 && httpCode != 200) {
    Serial.printf("[CMD] Error HTTP %d\n", httpCode);
  }
  
  http.end();
}

void executeCommand(String comando, JsonObject params) {
  if (comando == "move_forward") {
    int vel = params["velocidad_objetivo"] | 75;
    vel = constrain(vel, 0, 100);
    int speed = map(vel, 0, 100, 0, MAX_SPEED);
    
    Serial.printf("[EXEC] Adelante %d%%\n", vel);
    state.direction = "ADELANTE";
    state.speed = speed;
    lastMotorCommand = millis();  // Actualizar timestamp de comando
    moveForward(speed);
    
  } else if (comando == "move_reverse") {
    int vel = params["velocidad_objetivo"] | 75;
    vel = constrain(vel, 0, 100);
    int speed = map(vel, 0, 100, 0, MAX_SPEED);
    
    Serial.printf("[EXEC] Reversa %d%%\n", vel);
    state.direction = "ATRAS";
    state.speed = speed;
    lastMotorCommand = millis();  // Actualizar timestamp de comando
    moveBackward(speed);
    
  } else if (comando == "stop") {
    Serial.println("[EXEC] Deteniendo");
    state.direction = "STOP";
    state.speed = 0;
    lastMotorCommand = millis();  // Actualizar timestamp de comando
    stopMotors();
    
  } else {
    Serial.printf("[EXEC] Comando desconocido: %s\n", comando.c_str());
  }
  
  updateOLEDStatus();
}

void updateState() {
  HTTPClient http;
  String url = String("http://") + PROXY_HOST + ":" + PROXY_PORT + 
               "/train/" + TRAIN_ID + "/state";
  
  http.begin(url);
  http.addHeader("Content-Type", "application/json");
  http.setTimeout(3000);
  
  StaticJsonDocument<256> doc;
  doc["direction"] = state.direction;
  doc["speed"] = map(state.speed, 0, MAX_SPEED, 0, 100);
  doc["motors_running"] = state.motorsRunning;
  doc["wifi_rssi"] = WiFi.RSSI();
  doc["uptime"] = millis();
  
  String jsonString;
  serializeJson(doc, jsonString);
  
  http.POST(jsonString);
  http.end();
}

void sendHeartbeat() {
  HTTPClient http;
  String url = String("http://") + PROXY_HOST + ":" + PROXY_PORT + 
               "/train/" + TRAIN_ID + "/heartbeat";
  
  http.begin(url);
  http.setTimeout(2000);
  http.POST("");
  http.end();
}

// ============== CONTROL DE MOTORES ==============

void moveForward(int speed) {
  state.motorsRunning = true;
  
  // Motor A adelante
  ledcWrite(IN1, speed);
  ledcWrite(IN2, 0);
  
  // Motor B adelante
  ledcWrite(IN3, speed);
  ledcWrite(IN4, 0);
}

void moveBackward(int speed) {
  state.motorsRunning = true;
  
  // Motor A atrás
  ledcWrite(IN1, 0);
  ledcWrite(IN2, speed);
  
  // Motor B atrás
  ledcWrite(IN3, 0);
  ledcWrite(IN4, speed);
}

void stopMotors() {
  state.motorsRunning = false;
  
  ledcWrite(IN1, 0);
  ledcWrite(IN2, 0);
  ledcWrite(IN3, 0);
  ledcWrite(IN4, 0);
}

// ============== OLED (OPCIONAL) ==============

void initOLED() {
  Wire.begin(SDA_PIN, SCL_PIN, 100000);
  delay(50);
  
  if (display.begin(SSD1306_SWITCHCAPVCC, SCREEN_ADDRESS)) {
    oledReady = true;
    display.setRotation(2);
    display.clearDisplay();
    display.setTextSize(1);
    display.setTextColor(WHITE);
    display.setCursor(10, 20);
    display.println("METRO BOGOTA");
    display.setCursor(20, 35);
    display.printf("TREN #%d", TRAIN_ID);
    display.display();
    delay(2000);
    Serial.println("[OLED] Inicializado OK");
  } else {
    oledReady = false;
    Serial.println("[OLED] No detectado - continuando sin pantalla");
  }
}

void updateOLEDStatus() {
  if (!oledReady) return;
  
  display.clearDisplay();
  display.setTextSize(1);
  display.setTextColor(WHITE);
  
  display.setCursor(20, 0);
  display.printf("TREN #%d", TRAIN_ID);
  display.drawLine(0, 12, 128, 12, WHITE);
  
  display.setCursor(0, 18);
  display.print("Dir: ");
  display.println(state.direction);
  
  display.setCursor(0, 28);
  int velPercent = map(state.speed, 0, MAX_SPEED, 0, 100);
  display.printf("Vel: %d%%", velPercent);
  
  display.setCursor(0, 38);
  display.printf("WiFi: %ddBm", WiFi.RSSI());
  
  display.setCursor(0, 48);
  display.printf("UP: %lus", millis()/1000);
  
  // Barra de velocidad
  int barWidth = (state.speed * 120) / MAX_SPEED;
  display.drawRect(2, 56, 124, 6, WHITE);
  if (barWidth > 0) {
    display.fillRect(3, 57, barWidth, 4, WHITE);
  }
  
  display.display();
}