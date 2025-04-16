import paho.mqtt.client as mqtt
import json
import mysql.connector

# ✅ MySQL bağlantısı
db = mysql.connector.connect(
    host="localhost",
    user="root",
    password="admin123",  # Örn: "" veya "1234"
    database="mqtt_verileri"
)
cursor = db.cursor()

# 🔧 MQTT Ayarları
MQTT_BROKER = "13.50.125.85"
MQTT_PORT = 1883
MQTT_USERNAME = "admin"
MQTT_PASSWORD = "admin123"
MQTT_TOPIC = "fandf/E0E2E651DB74/state"

# 📩 Mesaj geldiğinde çalışır
def on_message(client, userdata, msg):
    print(f"[{msg.topic}]")
    try:
        data = json.loads(msg.payload.decode())
        print("Gelen JSON veri:", data)

        # ✅ MySQL'e kaydet
        cursor.execute("INSERT INTO sensor_verileri (deger) VALUES (%s)", (json.dumps(data),))
        db.commit()
        print("✅ MySQL'e kaydedildi.")

    except Exception as e:
        print("❌ Hata:", e)

# 🔌 MQTT bağlantısı sağlandığında
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("✅ MQTT Broker'a başarıyla bağlanıldı!")
        client.subscribe(MQTT_TOPIC)
    else:
        print("❌ Bağlantı başarısız! Kod:", rc)

# MQTT istemcisi oluştur
client = mqtt.Client()
client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
client.on_connect = on_connect
client.on_message = on_message

# Bağlan ve dinlemeye başla
client.connect(MQTT_BROKER, MQTT_PORT, 60)
client.loop_forever()
