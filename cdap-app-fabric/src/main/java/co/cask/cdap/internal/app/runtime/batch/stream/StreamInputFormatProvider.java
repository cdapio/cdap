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

package co.cask.cdap.internal.app.runtime.batch.stream;

import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.data.batch.InputFormatProvider;
import co.cask.cdap.api.data.format.FormatSpecification;
import co.cask.cdap.api.stream.StreamEventData;
import co.cask.cdap.api.stream.StreamEventDecoder;
import co.cask.cdap.common.conf.ConfigurationUtil;
import co.cask.cdap.data.stream.AbstractStreamInputFormat;
import co.cask.cdap.data.stream.StreamUtils;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.data2.transaction.stream.StreamConfig;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.StreamId;
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

  private final Input.StreamInput streamInput;
  private final StreamId streamId;
  private final StreamAdmin streamAdmin;

  public StreamInputFormatProvider(NamespaceId namespaceId,
                                   Input.StreamInput streamInput, StreamAdmin streamAdmin) {
    this.streamId = namespaceId.stream(streamInput.getName());
    this.streamInput = streamInput;
    this.streamAdmin = streamAdmin;
  }

  public StreamId getStreamId() {
    return streamId;
  }

  /**
   * Sets the {@link StreamEventDecoder} to be used by the InputFormat for the given type. If the
   * {@link Input.StreamInput} already defined a {@link StreamEventDecoder} or {@link FormatSpecification},
   * this method is a no-op.
   *
   * @param configuration configuration to update
   * @param type type for {@link StreamEventData} to decode to
   * @return the same configuration map as in the argument.
   */
  public Map<String, String> setDecoderType(Map<String, String> configuration, Type type) {
    if (streamInput.getBodyFormatSpec() == null && streamInput.getDecoderType() == null) {
      Configuration hConf = new Configuration();
      hConf.clear();
      AbstractStreamInputFormat.inferDecoderClass(hConf, type);
      configuration.putAll(ConfigurationUtil.toMap(hConf));
    }
    return configuration;
  }

  @Override
  public String getInputFormatClassName() {
    return MapReduceStreamInputFormat.class.getName();
  }

  @Override
  public Map<String, String> getInputFormatConfiguration() {
    try {
      StreamConfig streamConfig = streamAdmin.getConfig(streamId);
      Location streamPath = StreamUtils.createGenerationLocation(streamConfig.getLocation(),
                                                                 StreamUtils.getGeneration(streamConfig));
      Configuration hConf = new Configuration();
      hConf.clear();

      AbstractStreamInputFormat.setStreamId(hConf, streamId);
      AbstractStreamInputFormat.setTTL(hConf, streamConfig.getTTL());
      AbstractStreamInputFormat.setStreamPath(hConf, streamPath.toURI());
      AbstractStreamInputFormat.setTimeRange(hConf, streamInput.getStartTime(),
                                             streamInput.getEndTime());
      FormatSpecification formatSpec = streamInput.getBodyFormatSpec();
      if (formatSpec != null) {
        AbstractStreamInputFormat.setBodyFormatSpecification(hConf, formatSpec);
      } else {
        String decoderType = streamInput.getDecoderType();
        if (decoderType != null) {
          AbstractStreamInputFormat.setDecoderClassName(hConf, decoderType);
        }
      }

      return ConfigurationUtil.toMap(hConf);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }
}
