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

package io.cdap.cdap.logging.appender.remote;


import com.google.common.hash.Hashing;
import com.google.common.net.HttpHeaders;
import com.google.inject.Inject;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.logging.appender.AbstractLogPublisher;
import io.cdap.cdap.logging.appender.LogAppender;
import io.cdap.cdap.logging.appender.LogMessage;
import io.cdap.cdap.logging.appender.kafka.LogPartitionType;
import io.cdap.cdap.logging.serialize.LoggingEventSerializer;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
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
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Remote log appender to push logs to log saver.
 */
public class RemoteLogAppender extends LogAppender {
  private static final String APPENDER_NAME = "RemoteLogAppender";

  private final CConfiguration cConf;
  private final DiscoveryServiceClient discoveryServiceClient;
  private final AtomicReference<RemoteLogPublisher> publisher;

  @Inject
  public RemoteLogAppender(CConfiguration cConf, DiscoveryServiceClient discoveryServiceClient) {
    setName(APPENDER_NAME);
    this.cConf = cConf;
    this.discoveryServiceClient = discoveryServiceClient;
    this.publisher = new AtomicReference<>();
  }

  @Override
  public void start() {
    RemoteLogPublisher publisher = new RemoteLogPublisher(cConf, discoveryServiceClient);
    Optional.ofNullable(this.publisher.getAndSet(publisher)).ifPresent(RemoteLogPublisher::stopAndWait);
    publisher.startAndWait();
    addInfo("Successfully started " + APPENDER_NAME);
    super.start();
  }

  @Override
  public void stop() {
    super.stop();
    Optional.ofNullable(this.publisher.getAndSet(null)).ifPresent(RemoteLogPublisher::stopAndWait);
    addInfo("Successfully stopped " + APPENDER_NAME);
  }

  @Override
  protected void appendEvent(LogMessage logMessage) {
    logMessage.prepareForDeferredProcessing();
    logMessage.getCallerData();

    try {
      publisher.get().addMessage(logMessage);
    } catch (InterruptedException e) {
      addInfo("Interrupted when adding log message to queue: " + logMessage.getFormattedMessage());
    }
  }

  /**
   * Publisher service to publish logs to log saver. The publisher can be called by multiple threads so it is thread
   * safe.
   */
  private final class RemoteLogPublisher extends AbstractLogPublisher<Map.Entry<Integer, ByteBuffer>> {

    private final int numPartitions;
    private final ThreadLocal<LoggingEventSerializer> loggingEventSerializer;
    private final LogPartitionType logPartitionType;
    private final DatumWriter<List<ByteBuffer>> datumWriter;
    private final RemoteClient remoteClient;

    private RemoteLogPublisher(CConfiguration cConf, DiscoveryServiceClient discoveryServiceClient) {
      super(cConf.getInt(Constants.Logging.APPENDER_QUEUE_SIZE, 512),
            RetryStrategies.fromConfiguration(cConf, "system.log.process."));
      this.numPartitions = cConf.getInt(Constants.Logging.NUM_PARTITIONS);
      this.loggingEventSerializer = ThreadLocal.withInitial(LoggingEventSerializer::new);
      this.logPartitionType =
        LogPartitionType.valueOf(cConf.get(Constants.Logging.LOG_PUBLISH_PARTITION_KEY).toUpperCase());
      // DatumWriter stores schema in non final variable. However, this schem will not change per thread. So we are
      // not using ThreadLocal for datumWriter
      this.datumWriter = new GenericDatumWriter<>(Schema.createArray(Schema.create(Schema.Type.BYTES)));
      this.remoteClient = new RemoteClient(discoveryServiceClient, Constants.Service.LOG_BUFFER_SERVICE,
                                           new DefaultHttpRequestConfig(false), "/v1/logs");
    }

    @Override
    protected Map.Entry<Integer, ByteBuffer> createMessage(LogMessage logMessage) {
      String partitionKey = logPartitionType.getPartitionKey(logMessage.getLoggingContext());
      int partition = partition(partitionKey, numPartitions);
      return new AbstractMap.SimpleEntry<>(partition,
                                           ByteBuffer.wrap(loggingEventSerializer.get().toBytes(logMessage)));
    }

    @Override
    protected void publish(List<Map.Entry<Integer, ByteBuffer>> logMessages) throws Exception {
      // Group the log messages by partition and then publish all messages to their respective partitions
      Map<Integer, List<ByteBuffer>> partitionedMessages = new HashMap<>();
      for (Map.Entry<Integer, ByteBuffer> logMessage : logMessages) {
        List<ByteBuffer> messages = partitionedMessages.computeIfAbsent(logMessage.getKey(), k -> new ArrayList<>());
        messages.add(logMessage.getValue());
      }

      try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
        for (Map.Entry<Integer, List<ByteBuffer>> partition : partitionedMessages.entrySet()) {
          encodeEvents(os, datumWriter, partition.getValue());
          HttpRequest request = remoteClient.requestBuilder(HttpMethod.POST,
                                                            "/partitions/" + partition.getKey() + "/publish")
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

  private void encodeEvents(OutputStream os, DatumWriter<List<ByteBuffer>> datumWriter,
                            List<ByteBuffer> events) throws IOException {
    Encoder encoder = EncoderFactory.get().directBinaryEncoder(os, null);
    datumWriter.write(events, encoder);
  }
}
