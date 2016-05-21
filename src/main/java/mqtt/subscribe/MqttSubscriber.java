package mqtt.subscribe;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import commons.ConfigManager;

public class MqttSubscriber  implements MqttCallback {
    private final static Logger L = LoggerFactory.getLogger(MqttSubscriber.class);

    /**
     * MQTTブローカーとの接続を失った時に呼び出される.
     */
    @Override
    public void connectionLost(Throwable cause) {
        L.warn("Connection lost!");
        //再接続がしたかったらここに処理を書く
        System.exit(1);
    }
 
    /**
     * メッセージの送信が完了したときに呼ばれるCallback.
     */
    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
        //Subscribe側からは呼び出されない？
    }

    /**
     * メッセージを受信したときに呼ばれるCallback。SkyOnDemandではスクリプトにメッセージを渡す処理を行う。
     */
    @Override
    public void messageArrived(String topic, MqttMessage message) throws MqttException {
        L.info("Message arrived");
        L.info("Topic:", topic);
        L.info("Message: " + new String(message.getPayload()));
    }

    public static void main(String[] args) throws InterruptedException {
        try {
            MqttSubscriber subscriber = new MqttSubscriber();
            subscriber.subscribe();
        } catch(MqttException me) {
            L.error("reason: {} ", me.getReasonCode());
            L.error("message: {} ", me.getMessage());
            L.error("localize: {}", me.getLocalizedMessage());
            L.error("cause: {} ", me.getCause());
            L.error("exception: {}", me);
        }
    }

    /**
     * メッセージを受信する.
     * 標準入力があるまで接続し続ける.
     * 
     * @throws MqttException
     * @throws InterruptedException 
     */
    public void subscribe() throws MqttException, InterruptedException {
        
        //Subscribe設定
        final String broker       = ConfigManager.getBrokerConfig();
        final String topic        = String.valueOf(ConfigManager.getPubSubConfig("subscribe", "topic"));
        final int qos             = (int) ConfigManager.getPubSubConfig("subscribe", "qos");
        final String clientId     = String.valueOf(ConfigManager.getPubSubConfig("subscribe", "clientId"));

        MqttClient client = new MqttClient(broker, clientId, new MemoryPersistence());
        client.setCallback(this);
        MqttConnectOptions connOpts = new MqttConnectOptions();
        connOpts.setCleanSession(false);
         
        L.info("Connecting to broker: {}", broker);
        client.connect(connOpts);
        
        L.info("Connected and subscribing message: qos -> {}, topic -> {}", qos, topic);
        client.subscribe(topic, qos);
        
        L.info("Please press any key if you would disconnect to broker.");
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        try{
            //標準入力を受け取るまで待ち続ける
            br.readLine();
        }catch(IOException e){
            System.exit(1);
        }
        client.disconnect();
        L.info("Disconnected");
    }
}
