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

package co.cask.cdap.data.stream;

import co.cask.cdap.api.data.batch.InputFormatProvider;
import co.cask.cdap.api.data.format.FormatSpecification;
import co.cask.cdap.api.data.stream.StreamBatchReadable;
import co.cask.cdap.api.stream.StreamEventData;
import co.cask.cdap.api.stream.StreamEventDecoder;
import co.cask.cdap.common.conf.ConfigurationUtil;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.data2.transaction.stream.StreamConfig;
import co.cask.cdap.proto.Id;
import com.google.common.base.Throwables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.twill.filesystem.Location;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Map;

/**
 * A {@link InputFormatProvider} to provide {@link InputFormat} to read from stream.
 */
public class StreamInputFormatProvider implements InputFormatProvider {

  private final Id.Namespace namespaceId;
  private final StreamBatchReadable streamBatchReadable;
  private final StreamAdmin streamAdmin;

  public StreamInputFormatProvider(Id.Namespace namespaceId,
                                   StreamBatchReadable streamBatchReadable, StreamAdmin streamAdmin) {
    this.namespaceId = namespaceId;
    this.streamBatchReadable = streamBatchReadable;
    this.streamAdmin = streamAdmin;
  }

  /**
   * Returns the stream Id of the stream that will be consumed by InputFormat.
   */
  public Id.Stream getStreamId() {
    return Id.Stream.from(namespaceId, streamBatchReadable.getStreamName());
  }

  /**
   * Sets the {@link StreamEventDecoder} to be used by the InputFormat for the given type. If the
   * {@link StreamBatchReadable} already defined a {@link StreamEventDecoder} or {@link FormatSpecification},
   * this method is a no-op.
   *
   * @param configuration configuration to update
   * @param type type for {@link StreamEventData} to decode to
   * @return the same configuration map as in the argument.
   */
  public Map<String, String> setDecoderType(Map<String, String> configuration, Type type) {
    if (streamBatchReadable.getFormatSpecification() == null && streamBatchReadable.getDecoderType() == null) {
      Configuration hConf = new Configuration();
      hConf.clear();
      StreamInputFormat.inferDecoderClass(hConf, type);
      configuration.putAll(ConfigurationUtil.toMap(hConf));
    }
    return configuration;
  }

  @Override
  public String getInputFormatClassName() {
    return StreamInputFormat.class.getName();
  }

  @Override
  public Map<String, String> getInputFormatConfiguration() {
    Id.Stream streamId = Id.Stream.from(namespaceId, streamBatchReadable.getStreamName());
    try {
      StreamConfig streamConfig = streamAdmin.getConfig(streamId);
      Location streamPath = StreamUtils.createGenerationLocation(streamConfig.getLocation(),
                                                                 StreamUtils.getGeneration(streamConfig));
      Configuration hConf = new Configuration();
      hConf.clear();

      StreamInputFormat.setTTL(hConf, streamConfig.getTTL());
      StreamInputFormat.setStreamPath(hConf, streamPath.toURI());
      StreamInputFormat.setTimeRange(hConf, streamBatchReadable.getStartTime(), streamBatchReadable.getEndTime());
      FormatSpecification formatSpec = streamBatchReadable.getFormatSpecification();
      if (formatSpec != null) {
        StreamInputFormat.setBodyFormatSpecification(hConf, formatSpec);
      } else {
        String decoderType = streamBatchReadable.getDecoderType();
        if (decoderType != null) {
          StreamInputFormat.setDecoderClassName(hConf, decoderType);
        }
      }

      return ConfigurationUtil.toMap(hConf);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }
}
