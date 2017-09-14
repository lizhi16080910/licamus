/** 
 * @Title: KafkaConsumerTest.java 
 * @Package kafka 
 * @author LiFuqiang 
 * @date 2016年6月28日 下午5:50:04 
 * @version V1.0 
 * @Description: TODO(用一句话描述该文件做什么) 
 * Update Logs: 
 * **************************************************** * Name: * Date: * Description: ****************************************************** 
 */

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @ClassName: KafkaConsumerTest
 * @author LiFuqiang
 * @date 2016年6月28日 下午5:50:04
 * @Description: TODO(这里用一句话描述这个类的作用)
 */
public class KafkaConsumerTest {

    /**
     * @Title: main
     * @param args
     * @throws
     * @Description: TODO(这里用一句话描述这个方法的作用)
     */

    public static void main(String[] args) {
        String topic = "camus_test";
        Properties props = new Properties();
        props.put("auto.offset.reset", "smallest"); // 必须添加该配置，否则读取不到数据
        props.put("zookeeper.connect", "slave181:2181,slave182:2181,master185:2181");

        props.put("zookeeper.session.timeout.ms", "10000");
        props.put("zookeeper.sync.time.ms", "2000");
        props.put("auto.commit.interval.ms", "1000");
        props.put("group.id", "tt7");

        ConsumerConfig consumerConfig = new ConsumerConfig(props);
        ConsumerConnector consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);
        HashMap<String, Integer> map = new HashMap<String, Integer>();
        map.put(topic, 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> topicMessageStreams = consumerConnector
                .createMessageStreams(map);

        KafkaStream<byte[], byte[]> stream = topicMessageStreams.get(topic).get(0);
        int a = topicMessageStreams.size();
        int b = stream.size();

        ConsumerIterator<byte[], byte[]> it = stream.iterator();

        while (it.hasNext()) {
            System.out.println("ok");
            MessageAndMetadata<byte[], byte[]> msgAndMetadata = it.next();
            System.out.println(new String(msgAndMetadata.message()));
        }
         consumerConnector.shutdown();
    }

}
