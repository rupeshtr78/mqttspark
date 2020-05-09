import paho.mqtt.client as mqtt
import time

def on_log(client,userdata,level,buf):
    print("log: ",buf)

def on_connect(client,userdata,level,rc):
    if rc == 0:
        print("Connected "+str(rc))
        # client.subscribe("$SYS/#")
    else:
        print("Bad Connection Verify Broker "+str(rc))

def on_message(client, userdata, message):
    print("message received " ,str(message.payload.decode("utf-8")))
    print("message topic=",message.topic)
    print("message qos=",message.qos)
    print("message retain flag=",message.retain)
    

def on_disconnect(client,userdata,level,rc=0):
    print("Client Disconnected "+str(rc))

def main(broker, port,topic , message):
    # broker = "192.168.1.200"
    # port = 8883

    client = mqtt.Client("GPIOMQTT01")

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message
    client.on_log = on_log

    client.connect(broker,port,60)
    client.loop_start()

    # client.subscribe(topic)
    client.publish(topic , message)


    time.sleep(.3)
    client.loop_stop()
    # client.disconnect()
    # client.loop_forever()

if __name__ == '__main__':
    main()
