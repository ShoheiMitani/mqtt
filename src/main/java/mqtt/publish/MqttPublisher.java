package mqtt.publish;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import commons.ConfigManager;

public class MqttPublisher {
    private final static Logger L = LoggerFactory.getLogger(MqttPublisher.class);
    
    public static void main(String[] args) {

        //Publish設定
        final String broker       = ConfigManager.getBrokerConfig();
        final String topic        = String.valueOf(ConfigManager.getPubSubConfig("publish", "topic"));
        final int qos             = (int) ConfigManager.getPubSubConfig("publish", "qos");
        final String clientId     = String.valueOf(ConfigManager.getPubSubConfig("publish", "clientId"));
        //Publishするメッセージ内容
        String content      = args[0];

        MemoryPersistence persistence = new MemoryPersistence();

        try {
            MqttClient mqttClient = new MqttClient(broker, clientId, persistence);
            MqttConnectOptions connOpts = new MqttConnectOptions();
            connOpts.setCleanSession(false);

            L.info("Connecting to broker: {}", broker);
            mqttClient.connect(connOpts);

            L.info("Connected and publishing message: qos -> {}, message -> {}", qos, content);
            MqttMessage message = new MqttMessage(content.getBytes());
            message.setQos(qos);
            mqttClient.publish(topic, message);

            L.info("Message published and Disconneting broker");
            mqttClient.disconnect();
            L.info("Disconnected");

            System.exit(0);
        } catch(MqttException me) {
            L.error("reason: {} ", me.getReasonCode());
            L.error("message: {} ", me.getMessage());
            L.error("localize: {}", me.getLocalizedMessage());
            L.error("cause: {} ", me.getCause());
            L.error("exception: {}", me);
        }
    }
}
