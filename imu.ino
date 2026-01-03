#include <WiFi.h>
#include <PubSubClient.h>
#include <Wire.h>
#include <math.h>
#include <DHT.h>
#include <time.h>
#include <sys/time.h>

/* === YOUR SETTINGS === */
const char* WIFI_SSID = "Vignesh";
const char* WIFI_PASS = "12345678";
const char* MQTT_HOST = "172.20.10.3";   // your broker IP
const int   MQTT_PORT = 1883;
/* ===================== */

/* === TENANCY (Phase-1) ===
   For now you can keep these fixed.
   Later, you will provision via BLE / QR.
*/
const char* ORG_ID  = "org_demo";
const char* SITE_ID = "site_demo";

/* === WORKER MAPPING (PHASE-1) ===
   Flash BOTH devices with same WORKER_ID.
   Only change ROLE:
     - upper-body device  -> "trunk"
     - lower-body device  -> "knee"
*/
const char* WORKER_ID = "worker_001";
const char* ROLE      = "trunk";   // change to "knee" on the other ESP32

// pins
#define I2C_SDA   21
#define I2C_SCL   22
#define DHT_PIN    4
#define DHT_TYPE DHT11

// === HR SENSOR (analog) ===
#define HR_PIN    34

// mpu i2c address & registers
#define MPU_ADDR          0x68
#define REG_PWR_MGMT_1    0x6B
#define REG_SMPLRT_DIV    0x19
#define REG_CONFIG        0x1A
#define REG_GYRO_CONFIG   0x1B
#define REG_ACCEL_CONFIG  0x1C
#define REG_ACCEL_XOUT_H  0x3B
#define REG_WHO_AM_I      0x75

// ACCEL ±8g & GYRO ±500 dps
const float ACC_SENS = 4096.0f;
const float GYR_SENS = 65.5f;

WiFiClient espClient;
PubSubClient mqtt(espClient);
DHT dht(DHT_PIN, DHT_TYPE);

String DEVICE_ID;                // derived from MAC
const char* DEVICE_TOKEN = "tok_dev_001";

unsigned long lastPub = 0;
int lastHrRaw = 0;

/* ===================== TIME (NTP) ===================== */
static bool timeSynced = false;

uint64_t nowEpochMs() {
  struct timeval tv;
  gettimeofday(&tv, nullptr);
  return (uint64_t)tv.tv_sec * 1000ULL + (uint64_t)tv.tv_usec / 1000ULL;
}

void syncTimeNTP() {
  configTime(0, 0, "pool.ntp.org", "time.nist.gov");

  Serial.print("Syncing time");
  struct tm timeinfo;
  unsigned long start = millis();
  while (!getLocalTime(&timeinfo)) {
    Serial.print(".");
    delay(250);
    if (millis() - start > 15000) {
      Serial.println("\nNTP sync timeout (will retry later).");
      timeSynced = false;
      return;
    }
  }
  Serial.println("\nTime synced!");
  timeSynced = true;
}
/* ====================================================== */

/* --------- helpers for I2C raw --------- */
void mpuWrite(uint8_t reg, uint8_t val){
  Wire.beginTransmission(MPU_ADDR);
  Wire.write(reg);
  Wire.write(val);
  Wire.endTransmission(true);
}

uint8_t mpuRead8(uint8_t reg){
  Wire.beginTransmission(MPU_ADDR);
  Wire.write(reg);
  Wire.endTransmission(false);
  Wire.requestFrom(MPU_ADDR, (uint8_t)1);
  return Wire.available() ? Wire.read() : 0xFF;
}

void mpuReadBurst(uint8_t startReg, uint8_t* buf, uint8_t len){
  Wire.beginTransmission(MPU_ADDR);
  Wire.write(startReg);
  Wire.endTransmission(false);
  Wire.requestFrom(MPU_ADDR, len);
  for(uint8_t i=0; i<len && Wire.available(); i++){
    buf[i] = Wire.read();
  }
}

/* --------- WiFi / MQTT --------- */
void connectWiFi(){
  Serial.print("Connecting to WiFi");
  WiFi.begin(WIFI_SSID, WIFI_PASS);
  while (WiFi.status()!=WL_CONNECTED){
    delay(500);
    Serial.print(".");
  }
  Serial.print("\nWiFi connected. ESP IP: ");
  Serial.println(WiFi.localIP());
}

void connectMQTT(){
  while(!mqtt.connected()){
    Serial.print("Connecting MQTT...");
    if(mqtt.connect(DEVICE_ID.c_str())) Serial.println("OK");
    else {
      Serial.print("fail rc=");
      Serial.println(mqtt.state());
      delay(1500);
    }
  }
}

/* --------- MPU init & read --------- */
bool mpuInit(){
  mpuWrite(REG_PWR_MGMT_1, 0x00); delay(100);
  mpuWrite(REG_SMPLRT_DIV, 0x07);
  mpuWrite(REG_CONFIG, 0x04);
  mpuWrite(REG_GYRO_CONFIG, 0x08);
  mpuWrite(REG_ACCEL_CONFIG, 0x10);

  uint8_t who = mpuRead8(REG_WHO_AM_I);
  Serial.print("WHO_AM_I=0x");
  Serial.println(who, HEX);

  return (who==0x70 || who==0x71 || who==0x68);
}

void readAccelGyro(float &ax, float &ay, float &az, float &gx, float &gy, float &gz){
  uint8_t b[14];
  mpuReadBurst(REG_ACCEL_XOUT_H, b, 14);

  int16_t rax=(int16_t)((b[0]<<8)|b[1]);
  int16_t ray=(int16_t)((b[2]<<8)|b[3]);
  int16_t raz=(int16_t)((b[4]<<8)|b[5]);
  int16_t rgx=(int16_t)((b[8]<<8)|b[9]);
  int16_t rgy=(int16_t)((b[10]<<8)|b[11]);
  int16_t rgz=(int16_t)((b[12]<<8)|b[13]);

  ax = (float)rax/ACC_SENS;
  ay = (float)ray/ACC_SENS;
  az = (float)raz/ACC_SENS;
  gx = (float)rgx/GYR_SENS;
  gy = (float)rgy/GYR_SENS;
  gz = (float)rgz/GYR_SENS;
}

/* --------- setup / loop --------- */
void setup(){
  Serial.begin(115200);
  Wire.begin(I2C_SDA, I2C_SCL);
  dht.begin();

  pinMode(HR_PIN, INPUT);

  // Unique DEVICE_ID from MAC
  uint64_t chipid = ESP.getEfuseMac();
  uint32_t low = (uint32_t)(chipid & 0xFFFFFFFF);
  char idbuf[16];
  sprintf(idbuf, "exg-%08X", low);
  DEVICE_ID = String(idbuf);

  Serial.print("DEVICE_ID = "); Serial.println(DEVICE_ID);
  Serial.print("ORG_ID = ");    Serial.println(ORG_ID);
  Serial.print("SITE_ID = ");   Serial.println(SITE_ID);
  Serial.print("WORKER_ID = "); Serial.println(WORKER_ID);
  Serial.print("ROLE = ");      Serial.println(ROLE);

  WiFi.mode(WIFI_STA);
  delay(100);

  connectWiFi();
  syncTimeNTP();

  mqtt.setServer(MQTT_HOST, MQTT_PORT);
  mqtt.setBufferSize(1024);

  if(!mpuInit()){
    Serial.println("MPU not detected at 0x68 (check wiring). Continuing to send DHT only.");
  } else {
    Serial.println("MPU initialized (±8g, ±500 dps).");
  }
}

void loop(){
  if (WiFi.status()!=WL_CONNECTED) {
    connectWiFi();
    syncTimeNTP();
  }

  if (!mqtt.connected()) connectMQTT();
  mqtt.loop();

  static unsigned long lastTimeRetry = 0;
  if (!timeSynced && millis() - lastTimeRetry > 10000) {
    lastTimeRetry = millis();
    syncTimeNTP();
  }

  float ax=0, ay=0, az=0, gx=0, gy=0, gz=0;
  readAccelGyro(ax, ay, az, gx, gy, gz);

  float pitch = atan2(ay, sqrtf(ax*ax + az*az)) * 180.0f / PI;
  float roll  = atan2(-ax, az) * 180.0f / PI;
  float wz    = gz;
  float amag  = sqrtf(ax*ax + ay*ay + az*az);

  lastHrRaw = analogRead(HR_PIN);

  static unsigned long lastDht=0;
  static float tC=NAN, hum=NAN;
  if (millis()-lastDht > 1000){
    lastDht = millis();
    tC = dht.readTemperature();
    hum = dht.readHumidity();
  }

  if (millis()-lastPub > 1000){
    lastPub = millis();

    uint64_t ts = timeSynced ? nowEpochMs() : 0ULL;
    uint64_t ts_sec = (ts > 0) ? ((ts / 1000ULL) * 1000ULL) : 0ULL;

    // ✅ Better topic structure (future-ready)
    // Server already subscribes to ergoex/telemetry/# so this will still work
    String topic = String("ergoex/telemetry/")
                 + ORG_ID + "/"
                 + SITE_ID + "/"
                 + WORKER_ID + "/"
                 + ROLE + "/"
                 + DEVICE_ID;

    String payload = "{";
    payload += "\"org_id\":\"" + String(ORG_ID) + "\",";
    payload += "\"site_id\":\"" + String(SITE_ID) + "\",";
    payload += "\"device_id\":\"" + DEVICE_ID + "\",";
    payload += "\"worker_id\":\"" + String(WORKER_ID) + "\",";
    payload += "\"role\":\"" + String(ROLE) + "\",";
    payload += "\"token\":\"" + String(DEVICE_TOKEN) + "\",";

    payload += "\"t_ms\":" + String(millis()) + ",";
    payload += "\"ts\":" + String((unsigned long long)ts) + ",";
    payload += "\"ts_sec\":" + String((unsigned long long)ts_sec) + ",";
    payload += "\"time_ok\":" + String(timeSynced ? 1 : 0) + ",";

    payload += "\"pitch\":" + String(pitch,2) + ",";
    payload += "\"roll\":"  + String(roll,2)  + ",";
    payload += "\"wz\":"    + String(wz,2)    + ",";
    payload += "\"amag\":"  + String(amag,2)  + ",";
    payload += "\"hr_raw\":" + String(lastHrRaw) + ",";

    if (!isnan(tC))  payload += "\"temp_c\":" + String(tC,1) + ",";
    if (!isnan(hum)) payload += "\"hum\":"    + String(hum,1) + ",";

    if (payload.endsWith(",")) payload.remove(payload.length()-1);
    payload += "}";

    bool ok = mqtt.publish(topic.c_str(), payload.c_str());
    Serial.print("pub "); Serial.println(ok ? "OK" : "FAIL");
    Serial.println(payload);
  }

  delay(5);
}
