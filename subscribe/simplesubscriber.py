import paho.mqtt.client as mqtt

solace_url = ""
solace_port = 1883
solace_user = ""
solace_passwd = ""
solace_clientid = ""
solace_pi_topic = "devices/+/events"

def on_connect(client, userdata, flags, rc):  # The callback for when the client connects to the broker
    print("Connected with result code {0}".format(str(rc)))  # Print result of connection attempt
    client.subscribe(solace_pi_topic)  # Subscribe to the topic “digitest/test1”, receive any messages published on it

def on_message(client, userdata, msg):  # The callback for when a PUBLISH message is received from the server.
    print("Message received-> " + msg.topic + " " + str(msg.payload))  # Print a received msg

client = mqtt.Client(solace_clientid) # Create instance of client
client.username_pw_set(solace_user, password=solace_passwd)
client.on_connect = on_connect # Define callback function for successful connection
client.on_message = on_message # Define callback function for receipt of a message
client.connect(solace_url, solace_port, 60) # Connect to Solace Event Broker
client.loop_forever()  # Start networking daemon
