import RPi.GPIO as GPIO
import time
import mqttClient
import json
from datetime import datetime



lightPin = 4
buttonPin = 17

GPIO.setwarnings(False)
GPIO.setmode(GPIO.BCM)

lightPin = 4
buttonPin = 17
GPIO.setup(lightPin, GPIO.OUT,initial=GPIO.LOW)
GPIO.setup(buttonPin, GPIO.IN, pull_up_down=GPIO.PUD_UP)

def mqttPublishLines(buttonPin):
    topic = "rupesh/gpiotopic"
    broker = "192.168.1.200"
    port = 8883
    filepath = "/home/pi/mqtt/data/access_log.txt"

    file = open(filepath, "r")
    line = file.readlines()
    count = 0

    while True:
        if GPIO.input(buttonPin)==GPIO.LOW:
            time.sleep(0.1)
            count +=1
            payload = {"iottimestamp":str(datetime.utcnow()), "accesslogs":line[count]}
            message = json.dumps(payload)
            print("Button Pressed", GPIO.input(buttonPin))
            print("Event Detected", GPIO.event_detected(buttonPin))
            GPIO.output(lightPin, True)
            mqttClient.main(broker, port, topic, message)
        else:
            GPIO.output(lightPin, False)


def button_callback(buttonPin):
    if  GPIO.input(buttonPin):
        print("Button Pressed",GPIO.input(buttonPin))
        print("GPIO Event ",GPIO.event_detected(buttonPin))
        GPIO.output(lightPin, GPIO.input(buttonPin))
    else:
        print("Button not Pressed",GPIO.input(buttonPin))
        GPIO.output(lightPin, False)

#GPIO.add_event_detect(buttonPin,GPIO.BOTH,callback=button_callback,bouncetime=300)
GPIO.add_event_detect(buttonPin,GPIO.BOTH,callback=mqttPublishLines,bouncetime=300)

raw_input("Press Enter To Quit\n>")

try:
    print("Keyboard Interrupted!")
finally:
    GPIO.cleanup()

