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


import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.StringSerializer;

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
    
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("transactional.id", "my-transactional-id");
        KafkaProducer<String, String> producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());
    
        producer.initTransactions();
    
        try {
            producer.beginTransaction();
            for (int i = 0; i < 100; i++)
                producer.send(new ProducerRecord<>("my-topic", Integer.toString(i), Integer.toString(i)));
            producer.commitTransaction();
        } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
            // We can't recover from these exceptions, so our only option is to close the producer and exit.
            producer.close();
        } catch (KafkaException e) {
            // For all other exceptions, just abort the transaction and try again.
            producer.abortTransaction();
        }
        producer.close();
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
