We will create a  IOT Device / MQTT Client using Raspberry Pi .With a simple push button switch to simulate an event using bread board connected to the Pi device.

In the event of pressing button the device will publish access logs ( In our case we will read from a file line by line) to the broker. Later we will create a Spark Streaming application which will read the published topic and related messages into spark stream .Using Spark we will parse the log data ,count the status field using Spark window functions and watermark. Finally  we will write the stream of data to Cassandra sink.

More details of this implementation can be found here on my [blog](https://rupeshtr78.github.io/blog/)

