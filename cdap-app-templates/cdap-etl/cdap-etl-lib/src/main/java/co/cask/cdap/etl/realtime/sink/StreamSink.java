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

package co.cask.cdap.etl.realtime.sink;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.api.stream.StreamEventData;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.realtime.DataWriter;
import co.cask.cdap.etl.api.realtime.RealtimeSink;
import co.cask.cdap.etl.common.Properties;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Real-time sink for Streams
 */
@Plugin(type = "realtimesink")
@Name("Stream")
@Description("Real-time sink that outputs to a specified CDAP stream.")
public class StreamSink extends RealtimeSink<StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(StreamSink.class);

  private static final String NAME_DESC = "The name of the stream to output to. Must be a valid stream name. " +
    "The stream will be created if it does not exist.";
  private static final String BODY_FIELD_DESC = "Name of the field in the record that contains the data to be " +
    "written to the specified stream. The data could be in binary format as a byte array or a ByteBuffer. " +
    "It can also be a String. If unspecified, the 'body' key is used.";
  private static final String HEADERS_FIELD_DESC = "Name of the field in the record that contains headers. " +
    "Headers are presumed to be a map of string to string.";

  private final StreamConfig streamConfig;

  public StreamSink(StreamConfig streamConfig) {
    this.streamConfig = streamConfig;
  }

  /**
   * Config class for StreamSink.
   */
  public static class StreamConfig extends PluginConfig {

    @Description(NAME_DESC)
    private String name;

    @Name(Properties.Stream.HEADERS_FIELD)
    @Description(HEADERS_FIELD_DESC)
    @Nullable
    private String headersField;

    @Name(Properties.Stream.BODY_FIELD)
    @Description(BODY_FIELD_DESC)
    @Nullable
    private String bodyField;

    public StreamConfig() {
      this(null, Properties.Stream.DEFAULT_HEADERS_FIELD, Properties.Stream.DEFAULT_BODY_FIELD);
    }

    public StreamConfig(String name, String headersField, String bodyField) {
      this.name = name;
      this.headersField = headersField;
      this.bodyField = bodyField;
    }
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(streamConfig.name),
                                "Stream name should be non-null, non-empty.");
    pipelineConfigurer.addStream(new Stream(streamConfig.name));
  }

  @Override
  public int write(Iterable<StructuredRecord> structuredRecords, DataWriter dataWriter) throws Exception {
    int numRecordsWritten = 0;
    for (StructuredRecord structuredRecord : structuredRecords) {
      Schema schema = structuredRecord.getSchema();
      Object data = structuredRecord.get(streamConfig.bodyField);
      Object headers = structuredRecord.get(streamConfig.headersField);
      if (data == null) {
        LOG.debug("Found null data. Skipping record.");
        continue;
      }

      if (headers != null && !isHeadersSchemaPresentAndSupported(schema)) {
        LOG.debug("Headers found in input, however either the headers schema is not provided or the provided " +
                    "schema is not supported. Only a map of string keys and string values is supported. " +
                    "Skipping record.");
        continue;
      }

      Schema.Field dataSchemaField = schema.getField(streamConfig.bodyField);
      switch (dataSchemaField.getSchema().getType()) {
        case BYTES:
          numRecordsWritten += writeBytes(dataWriter, data, headers);
          break;
        case STRING:
          numRecordsWritten += writeString(dataWriter, data, headers);
          break;
        default:
          LOG.debug("Type {} is not supported for writing to stream", data.getClass().getName());
          break;
      }
    }
    return numRecordsWritten;
  }

  private boolean isHeadersSchemaPresentAndSupported(Schema recordSchema) {
    Schema.Field headersSchemaField = recordSchema.getField(streamConfig.headersField);
    if (headersSchemaField != null) {
      Map.Entry<Schema, Schema> mapSchema = headersSchemaField.getSchema().getMapSchema();
      return mapSchema.getKey().getType().equals(Schema.Type.STRING) &&
        mapSchema.getValue().getType().equals(Schema.Type.STRING);
    }
    return false;
  }

  private int writeBytes(DataWriter writer, Object data, Object headers) throws IOException {
    ByteBuffer buffer;
    if (data instanceof ByteBuffer) {
      buffer = (ByteBuffer) data;
    } else if (data instanceof byte []) {
      buffer = ByteBuffer.wrap((byte []) data);
    } else {
      LOG.debug("Type {} is not supported for writing to stream", data.getClass().getName());
      return 0;
    }
    if (headers != null && headers instanceof Map) {
      StreamEventData streamEventData = new StreamEventData((Map<String, String>) headers, buffer);
      writer.write(streamConfig.name, streamEventData);
    } else {
      writer.write(streamConfig.name, buffer);
    }
    return 1;
  }

  private int writeString(DataWriter writer, Object data, Object headers) throws IOException {
    if (headers != null && headers instanceof Map) {
      writer.write(streamConfig.name, (String) data, (Map<String, String>) headers);
    } else {
      writer.write(streamConfig.name, (String) data);
    }
    return 1;
  }
}
