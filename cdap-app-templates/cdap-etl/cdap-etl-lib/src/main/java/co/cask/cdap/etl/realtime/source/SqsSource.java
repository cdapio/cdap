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

package co.cask.cdap.etl.realtime.source;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.realtime.RealtimeContext;
import co.cask.cdap.etl.api.realtime.RealtimeSource;
import co.cask.cdap.etl.api.realtime.SourceState;
import com.amazon.sqs.javamessaging.SQSConnection;
import com.amazon.sqs.javamessaging.SQSConnectionFactory;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;

/**
 * Realtime source that reads from Amazon SQS.
 * TODO: CDAP-2978: Extend JMS source so this class can be deleted.
 */
@Plugin(type = "realtimesource")
@Name("AmazonSQS")
@Description("Amazon Simple Queue Service real-time source: emits a record with a field 'body' of type String.")
public class SqsSource extends RealtimeSource<StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(SqsSource.class);
  private static final String REGION_DESCRIPTION = "Region where the queue is located.";
  private static final String ACCESSKEY_DESCRIPTION = "Access Key of the AWS (Amazon Web Services) account to use.";
  private static final String ACCESSID_DESCRIPTION = "Access ID of the AWS (Amazon Web Services) account to use.";
  private static final String QUEUENAME_DESCRIPTION = "Name of the queue.";
  private static final String ENDPOINT_DESCRIPTION = "Endpoint of the SQS server to connect to. Omit this field to " +
    "connect to AWS (Amazon Web Services).";
  private static final Schema DEFAULT_SCHEMA = Schema.recordOf(
    "event",
    Schema.Field.of("body", Schema.of(Schema.Type.STRING))
  );
  private static final int MAX_MESSAGE_COUNT = 20;
  private static final int TIMEOUT_LENGTH = 1000;
  private final SqsConfig config;
  private SQSConnectionFactory connectionFactory;
  private MessageConsumer consumer;
  private Session session;
  private SQSConnection connection;

  public SqsSource(SqsConfig config) {
    this.config = config;
  }

  @Override
  public void initialize(RealtimeContext contex) {
    try {
      SQSConnectionFactory.Builder sqsBuild = SQSConnectionFactory.builder()
        .withRegion(Region.getRegion(Regions.fromName(config.region)));
      connectionFactory = config.endpoint == null ? sqsBuild.build() : sqsBuild.withEndpoint(config.endpoint).build();
      connection = connectionFactory.createConnection(config.accessID, config.accessKey);
      session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      consumer = session.createConsumer(session.createQueue(config.queueName));
      connection.start();
    } catch (Exception e) {
      if (session != null) {
        try {
          session.close();
        } catch (Exception ex1) {
          LOG.warn("Exception when closing session", ex1);
        }
      }
      if (connection != null) {
        try {
          connection.close();
        } catch (Exception ex2) {
          LOG.warn("Exception when closing connection", ex2);
        }
      }
      if (consumer != null) {
        try {
          consumer.close();
        } catch (Exception ex3) {
          LOG.warn("Exception when closing consumer", ex3);
        }
      }
      LOG.error("Failed to connect to SQS");
      throw new IllegalStateException("Could not connect to SQS.");
    }
  }

  @Override
  public SourceState poll(Emitter<StructuredRecord> writer, SourceState currentState) throws Exception {
    Message msg;
    int count = 0;
    while ((msg = consumer.receive(TIMEOUT_LENGTH)) != null && count < MAX_MESSAGE_COUNT) {
      String text = ((TextMessage) msg).getText();
      if (text.isEmpty()) {
        msg.acknowledge();
        continue;
      }

      StructuredRecord record = StructuredRecord.builder(DEFAULT_SCHEMA)
        .set("body", text)
        .build();
      writer.emit(record);
      msg.acknowledge();
      count++;
    }
    return currentState;
  }

  @Override
  public void destroy() {
    try {
      consumer.close();
      session.close();
      connection.close();
    } catch (Exception ex) {
      throw new RuntimeException("Exception on closing SQS connection: " + ex.getMessage(), ex);
    }
  }

  /**
   * Config class for {@link SqsSource}
   */
  public static class SqsConfig extends PluginConfig {
    @Name("region")
    @Description(REGION_DESCRIPTION)
    private String region;

    @Name("accessKey")
    @Description(ACCESSKEY_DESCRIPTION)
    private String accessKey;

    @Name("accessID")
    @Description(ACCESSID_DESCRIPTION)
    private String accessID;

    @Name("queueName")
    @Description(QUEUENAME_DESCRIPTION)
    private String queueName;

    @Name("endpoint")
    @Nullable
    @Description(ENDPOINT_DESCRIPTION)
    private String endpoint;

    public SqsConfig(String region, String accessKey, String accessID, String queueName, @Nullable String endpoint) {
      this.region = region;
      this.accessID = accessID;
      this.accessKey = accessKey;
      this.queueName = queueName;
      this.endpoint = endpoint;
    }
  }
}
