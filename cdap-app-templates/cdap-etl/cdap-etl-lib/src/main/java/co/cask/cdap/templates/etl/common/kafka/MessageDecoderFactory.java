package co.cask.cdap.templates.etl.common.kafka;

import org.apache.hadoop.mapreduce.JobContext;

import java.util.Map.Entry;
import java.util.Properties;


public class MessageDecoderFactory {

  public static MessageDecoder<?, ?> createMessageDecoder(JobContext context, String topicName) {
    MessageDecoder<?, ?> decoder;
    try {
      decoder = (MessageDecoder<?, ?>) EtlInputFormat.getMessageDecoderClass(context, topicName).newInstance();

      Properties props = new Properties();
      for (Entry<String, String> entry : context.getConfiguration()) {
        props.put(entry.getKey(), entry.getValue());
      }

      decoder.init(props, topicName);

      return decoder;
    } catch (Exception e) {
      throw new MessageDecoderException(e);
    }
  }

}
