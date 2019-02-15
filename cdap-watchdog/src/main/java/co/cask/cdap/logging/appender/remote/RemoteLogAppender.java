/*
 * Copyright © 2019 Cask Data, Inc.
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

package co.cask.cdap.logging.appender.remote;


import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.http.DefaultHttpRequestConfig;
import co.cask.cdap.common.internal.remote.RemoteClient;
import co.cask.cdap.common.service.RetryStrategies;
import co.cask.cdap.logging.appender.AbstractLogPublisher;
import co.cask.cdap.logging.appender.LogAppender;
import co.cask.cdap.logging.appender.LogMessage;
import co.cask.cdap.logging.appender.kafka.LogPartitionType;
import co.cask.cdap.logging.serialize.LogSchema;
import co.cask.cdap.logging.serialize.LoggingEventSerializer;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;
import com.google.common.hash.Hashing;
import com.google.common.net.HttpHeaders;
import com.google.inject.Inject;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.twill.discovery.DiscoveryServiceClient;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Remote log appender to push logs to log saver.
 */
public class RemoteLogAppender extends LogAppender {
  private static final String APPENDER_NAME = "RemoteLogAppender";
  private final RemoteClient remoteClient;
  private final RemoteLogPublisher publisher;

  @Inject
  public RemoteLogAppender(CConfiguration cConf, DiscoveryServiceClient discoveryServiceClient) {
    setName(APPENDER_NAME);
    this.publisher = new RemoteLogPublisher(cConf);
    this.remoteClient = new RemoteClient(discoveryServiceClient, Constants.Service.LOG_BUFFER_SERVICE,
                                         new DefaultHttpRequestConfig(), "/v1/");
  }

  @Override
  public void start() {
    publisher.startAndWait();
    addInfo("Successfully started " + APPENDER_NAME);
    super.start();
  }

  @Override
  public void stop() {
    publisher.stopAndWait();
    addInfo("Successfully stopped " + APPENDER_NAME);
    super.stop();
  }

  @Override
  protected void appendEvent(LogMessage logMessage) {
    logMessage.prepareForDeferredProcessing();
    logMessage.getCallerData();

    try {
      publisher.addMessage(logMessage);
    } catch (InterruptedException e) {
      addInfo("Interrupted when adding log message to queue: " + logMessage.getFormattedMessage());
    }
  }

  /**
   * Publisher service to publish logs to log saver.
   */
  private final class RemoteLogPublisher extends AbstractLogPublisher<Map.Entry<Integer, byte[]>> {

    private final int numPartitions;
    private final LoggingEventSerializer loggingEventSerializer;
    private final LogPartitionType logPartitionType;
    private final DatumWriter<GenericRecord> datumWriter;

    private RemoteLogPublisher(CConfiguration cConf) {
      super(cConf.getInt(Constants.Logging.APPENDER_QUEUE_SIZE, 512),
            RetryStrategies.fromConfiguration(cConf, "system.log.process."));
      this.numPartitions = cConf.getInt(Constants.Logging.NUM_PARTITIONS);
      this.loggingEventSerializer = new LoggingEventSerializer();
      this.logPartitionType =
        LogPartitionType.valueOf(cConf.get(Constants.Logging.LOG_PUBLISH_PARTITION_KEY).toUpperCase());
      datumWriter = new GenericDatumWriter<>(LogSchema.LogBufferRequest.SCHEMA);
    }

    @Override
    protected Map.Entry<Integer, byte[]> createMessage(LogMessage logMessage) {
      String partitionKey = logPartitionType.getPartitionKey(logMessage.getLoggingContext());
      int partition = partition(partitionKey, numPartitions);
      return new AbstractMap.SimpleEntry<>(partition, loggingEventSerializer.toBytes(logMessage));
    }

    @Override
    protected void publish(List<Map.Entry<Integer, byte[]>> logMessages) throws Exception {
      // Group the log messages by partition and then publish all messages to their respective partitions
      Map<Integer, List<byte[]>> partitionedMessages = new HashMap<>();
      for (Map.Entry<Integer, byte[]> logMessage : logMessages) {
        List<byte[]> messages = partitionedMessages.computeIfAbsent(logMessage.getKey(), k -> new ArrayList<>());
        messages.add(logMessage.getValue());
      }

      try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
        for (Map.Entry<Integer, List<byte[]>> partition : partitionedMessages.entrySet()) {
          encodeEvents(os, datumWriter, partition.getKey(), partition.getValue());
          HttpRequest request = remoteClient.requestBuilder(HttpMethod.POST, "process")
            .addHeader(HttpHeaders.CONTENT_TYPE, "avro/binary")
            .withBody(ByteBuffer.wrap(os.toByteArray())).build();
          HttpResponse response = remoteClient.execute(request);
          // if something went wrong, throw exception to retry
          if (response.getResponseCode() != HttpURLConnection.HTTP_OK) {
            throw new IOException(String.format("Could not append logs for partition %s", partition));
          }
          os.reset();
        }
      }
    }

    @Override
    protected void logError(String errorMessage, Exception exception) {
      // Log using the status manager
      addError(errorMessage, exception);
    }
  }

  /**
   * Similar to StringPartitioner, but StringPartitioner class implements kafka Partitioner. As we do not want kafka
   * dependencies in this class, this method is added here.
   */
  private static int partition(String key, int numPartitions) {
    return Math.abs(Hashing.md5().hashString(key).asInt()) % numPartitions;
  }

  private void encodeEvents(OutputStream os, DatumWriter<GenericRecord> datumWriter,
                            int partition, List<byte[]> events) throws IOException {
    GenericRecord record = new GenericData.Record(LogSchema.LogBufferRequest.SCHEMA);
    record.put("partition", partition);
    record.put("events", events.stream().map(ByteBuffer::wrap).collect(Collectors.toList()));
    Encoder encoder = EncoderFactory.get().directBinaryEncoder(os, null);
    datumWriter.write(record, encoder);
  }
}
