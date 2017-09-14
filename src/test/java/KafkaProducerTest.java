/** 
 * @Title: KafkaProducerTest.java 
 * @Package kafka 
 * @author LiFuqiang 
 * @date 2016年6月28日 上午10:53:26 
 * @version V1.0 
 * @Description: TODO(用一句话描述该文件做什么) 
 * Update Logs: 
 * **************************************************** * Name: * Date: * Description: ****************************************************** 
 */

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.Partitioner;
import kafka.producer.ProducerConfig;
import kafka.utils.VerifiableProperties;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

/**
 * @ClassName: KafkaProducerTest
 * @author LiFuqiang
 * @date 2016年6月28日 上午10:53:26
 * @Description: TODO(这里用一句话描述这个类的作用)
 */
public class KafkaProducerTest {

    /**
     * @Title: main
     * @param args
     * @throws IOException
     * @throws InterruptedException
     * @throws
     * @Description: TODO(这里用一句话描述这个方法的作用)
     */
    public static void main(String[] args) throws IOException, InterruptedException {

        String topic = "cdnlog_xns_topic";

        Properties props = new Properties();

        props.put("metadata.broker.list",
                "192.168.100.60:9092,192.168.100.61:9092,192.168.100.62:9092,192.168.100.63:9092,192.168.100.64:9092,192.168.100.65:9092,192.168.100.66:9092,192.168.100.67:9092,192.168.100.68:9092,192.168.100.69:9092");
        // props.put("zookeeper.connect",
        // "192.168.100.185:2181,192.168.100.181:2181,192.168.100.182:2181/kafka");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("key.serializer.class", "kafka.serializer.StringEncoder");
        // props.put("partitioner.class", "example.producer.SimplePartitioner");
        props.put("request.required.acks", "1");

        ProducerConfig config = new ProducerConfig(props);

        Producer<String, String> producer = new Producer<String, String>(config);
        File file = new File("F:\\test\\kans_query_stat.log");
        BufferedReader reader = null;
        int i = 0;
        try {
            while (true) {
                reader = new BufferedReader(new FileReader(file));
                String line = null;
                while ((line = reader.readLine()) != null) {
                    String msg = "ctl-zj-122-228-198-148 " + line;
                    KeyedMessage<String, String> data = new KeyedMessage<String, String>(
                            "cdnlog_xns_topic", msg);
                    producer.send(data);
                    System.out.println(msg);
                    Thread.sleep(10);
                    i++;
                    System.out.println(i);
                }
                reader.close();
            }
        }
        catch (Exception e) {
            producer.close();
            // reader.close();
        }
    }

    public static class SimplePartitioner implements Partitioner {
        public SimplePartitioner(VerifiableProperties props) {

        }

        public int partition(Object key, int a_numPartitions) {
            int partition = 0;
            String stringKey = (String) key;
            int offset = stringKey.lastIndexOf('.');
            if (offset > 0) {
                partition = Integer.parseInt(stringKey.substring(offset + 1)) % a_numPartitions;
            }
            return partition;
        }
    }
}
