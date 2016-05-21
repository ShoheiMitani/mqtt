package commons;

import java.util.Map;
import java.util.Objects;

import org.yaml.snakeyaml.Yaml;

public class ConfigManager {
    /**
     * YAMLファイルから全ての設定値を取得する.
     * @return map 取得した設定値
     */
    private static Map<String,Object> loadAll() {
        Yaml yaml = new Yaml();
        @SuppressWarnings("unchecked")
        final Map<String,Object> configMap = yaml.loadAs(ClassLoader.getSystemResourceAsStream("conf/mqtt-config.yaml"), Map.class);
        return configMap;
    }
    
    /**
     * MQTTブローカーの設定値を取得する.
     * @return broker MQTTブローカーのサーバー情報
     */
    public static String getBrokerConfig(){
        final Map<String,Object> configMap = loadAll();
        String broker = String.valueOf(configMap.get("broker"));
        Objects.requireNonNull(broker,"No broker definition in yaml file.");
        return broker;
    }
    
    /**
     * 引数に指定された内容に従ってPublishかSubscribeの設定情報を取得する.
     * @param pubSubType 取得したい情報元（Publish | Subscribe）を指定する
     *    "publish"か"subscribe"を指定すること
     * @param target 取得したい設定値を指定する
     */
    @SuppressWarnings("unchecked")
    public static Object getPubSubConfig(String pubSubType, String target){
        
        Objects.requireNonNull(pubSubType);
        Objects.requireNonNull(target);
        
        final Map<String, Object> pubSubConfig = (Map<String, Object>) loadAll().get(pubSubType);
        Object element = pubSubConfig.get(target);
        Objects.requireNonNull(element,"Can not find element");
        return element;
    }
}
