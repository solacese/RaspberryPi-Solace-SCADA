
import paho.mqtt.client as mqtt
import time
import random
import json

def publish(client,sensor,topic,qos,simulated_reading,trend):
    simulated_reading = simulated_reading + trend * random.normalvariate(0.01, 0.005)
    payload = {"timestamp": int(time.time()), "device": sensor, sensor: simulated_reading}
    jsonpayload_sensor = json.dumps(payload, indent=4)
    client.publish(topic, jsonpayload_sensor, qos=qos)
    print("Published to topic {}: \n{}".format(topic, jsonpayload_sensor))

def main():
    # Connection parms for Solace Event broker
    solace_url = ""
    solace_port = 1883
    solace_user = ""
    solace_passwd = ""
    solace_clientid = "vats_id"

    # Sensor Topics
    solace_topic_nox = "devices/nox/events"
    solace_topic_pressure = "devices/pressure/events"
    solace_topic_sox = "devices/sox/events"
    solace_topic_level = "devices/level/events"

    # Instantiate/connect to mqtt client
    client = mqtt.Client(solace_clientid)
    client.username_pw_set(username=solace_user, password=solace_passwd)
    print("Connecting to solace {}:{} as {}".format(solace_url, solace_port, solace_user))
    client.connect(solace_url, port=solace_port)
    client.loop_start()

    simulated_nox = 50 + random.random() * 20
    simulated_sox = 40 + random.random() * 20
    simulated_pressure = 30 + random.random() * 20
    simulated_level = 15 + random.random() * 20

    if random.random() > 0.5:
        sensor_trend = +1  # value will slowly rise
    else:
        sensor_trend = -1  # value will slowly fall

    # Publish num_messages mesages to the MQTT bridge once per second.
    while True:
        publish(client, "sox", solace_topic_sox, 1,simulated_sox, sensor_trend)
        publish(client, "nox", solace_topic_nox, 1, simulated_nox, sensor_trend)
        publish(client, "level", solace_topic_level, 1, simulated_level, sensor_trend)
        publish(client, "pressure", solace_topic_pressure, 1, simulated_pressure, sensor_trend)

        time.sleep(2)
    client.loop_stop()
    client.disconnect()

    return {
        'statusCode': 200,
        'body': "Success"
    }

main()
