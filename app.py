import paho.mqtt.client as mqtt
import json
import mysql.connector

# ✅ MySQL bağlantısı
db = mysql.connector.connect(
    host="localhost",  # Docker veya MySQL'inizin çalıştığı IP veya 'localhost'
    user="root",  # MySQL kullanıcı adı
    password="admin123",  # Veritabanı şifreniz
    database="mqtt_verileri"  # Kullanacağınız veritabanı
)
cursor = db.cursor()

# 🔧 MQTT Ayarları
MQTT_BROKER = "mqtt.wittech.com"  # Sizin MQTT Broker IP'niz
MQTT_PORT = 1883  # Port
MQTT_USERNAME = "wittech-test"  # Kullanıcı adı
MQTT_PASSWORD = "Wittech123"  # Şifre
MQTT_TOPIC = "fandf/B8D614A23698/state"  # Konu adı (topic)

# 📩 Mesaj geldiğinde çalışır
def on_message(client, userdata, msg):
    print(f"[{msg.topic}]")
    try:
        data = json.loads(msg.payload.decode())  # Mesajı JSON formatında çöz
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
        client.subscribe(MQTT_TOPIC)  # Konuyu dinlemeye başla
    else:
        print("❌ Bağlantı başarısız! Kod:", rc)

# MQTT istemcisi oluştur
client = mqtt.Client()
client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)  # MQTT Broker'a bağlanmak için kullanıcı adı ve şifre
client.on_connect = on_connect
client.on_message = on_message  # Mesaj geldiğinde çalışacak fonksiyon

# Bağlan ve dinlemeye başla
client.connect(MQTT_BROKER, MQTT_PORT, 60)
client.loop_forever()  # Sonsuza kadar dinlemeye devam et
