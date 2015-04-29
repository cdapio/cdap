/*
 * Copyright © 2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.templates.etl.common.kafka;

import org.apache.hadoop.mapreduce.JobContext;

import java.util.Map.Entry;
import java.util.Properties;

/**
 * Message Decoder Factory.
 */
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
