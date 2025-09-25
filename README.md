# Reactor MQTT

reactor-netty + netty-mqtt-codec = reactor-mqtt

## Server

```java
// create server
DisposableServer server = MqttServer
.create()
.host("0.0.0.0")
.port(1883)
.handle(connection->{

connection
 .handlePublishing(message->...)
 .handleSubscribe(subscription->...)
 .handleUnsubscrie(topic-> ... )

return validate(connection).then(connection.accept())

})
.bindNow();
```


## Client

```java

// create client
MqttClientConnection conn = MqttClient
.create()
.host("127.0.0.1")
.port(1883)
// handle all publishing from server
.handlePublishing(pub->{
    
})
.connectNow();

// subscribe and handle
Disposable disp =  conn.subscribe("/topic",publishing->{  .... });

conn.publish(topic,qos);

```
