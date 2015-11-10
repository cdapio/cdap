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

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.InvalidEntry;
import co.cask.cdap.etl.api.realtime.SourceState;
import co.cask.cdap.etl.common.MockRealtimeContext;
import co.cask.cdap.etl.realtime.jms.JmsProvider;
import co.cask.cdap.etl.realtime.source.JmsSource.JmsPluginConfig;
import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicConnection;

/**
 * Unit test for JMS ETL realtime source
 */
public class JmsSourceTest {
  private static final Logger LOG = LoggerFactory.getLogger(JmsSourceTest.class);

  private final int sessionAckMode = Session.AUTO_ACKNOWLEDGE;

  private JmsSource jmsSource;
  private JmsProvider jmsProvider;

  private String originalMessage;

  @Before
  public void beforeTest() {
    jmsSource = null;
  }

  @After
  public void afterTest() {
    if (jmsSource != null) {
      jmsSource.destroy();
    }
    originalMessage = "NO_MATCH";
  }

  @Test
  public void testSimpleQueueMessages() throws Exception {
    initializeJmsSource("dynamicQueues/CDAP.QUEUE", 50, "org.apache.activemq.jndi.ActiveMQInitialContextFactory",
                        "vm://localhost?broker.persistent=false");

    jmsProvider = new MockJmsProvider("dynamicQueues/CDAP.QUEUE");
    jmsSource.setJmsProvider(jmsProvider);
    jmsSource.setSessionAcknowledgeMode(sessionAckMode);
    jmsSource.initialize(new MockRealtimeContext());

    ConnectionFactory connectionFactory = jmsProvider.getConnectionFactory();
    QueueConnection queueConn = null;
    try {
      queueConn = (QueueConnection) connectionFactory.createConnection();
      Queue queueDestination = (Queue) jmsProvider.getDestination();

      // Let's start the Connection
      queueConn.start();
      sendMessage(queueConn, queueDestination, "Queue with Mock Provider:" + queueDestination.getQueueName());

      // Verify if it is valid
      verifyEmittedText(jmsSource);

    } finally {
      if (queueConn != null) {
        try {
          queueConn.close();
        } catch (JMSException e) {
          LOG.error("Exception when closing Queue connection.");
        }
      }
    }
  }

  @Test
  public void testSimpleTopicMessages() throws Exception {
    initializeJmsSource("dynamicTopics/CDAP.TOPIC", 50, "org.apache.activemq.jndi.ActiveMQInitialContextFactory",
                        "vm://localhost?broker.persistent=false");

    jmsProvider = new MockJmsProvider("dynamicTopics/CDAP.TOPIC");
    jmsSource.setJmsProvider(jmsProvider);
    jmsSource.setSessionAcknowledgeMode(sessionAckMode);
    jmsSource.initialize(new MockRealtimeContext());

    ConnectionFactory connectionFactory = jmsProvider.getConnectionFactory();
    TopicConnection topicConn = null;
    try {
      topicConn = (TopicConnection) connectionFactory.createConnection();

      Topic topicDestination = (Topic) jmsProvider.getDestination();

      // Let's start the Connection
      topicConn.start();
      sendMessage(topicConn, topicDestination, "Topic with Mock Provider:" + topicDestination.getTopicName());

      // Verify if it is valid
      verifyEmittedText(jmsSource);
    } finally {
      if (topicConn != null) {
        try {
          topicConn.close();
        } catch (JMSException e) {
          LOG.error("Exception when closing Topic connection.");
        }
      }
    }
  }

  @Test
  public void testJndiBasedJmsProvider() throws Exception {
    // Create ActiveMQ ConnectionFactory for the JNDI based JmsProvider
    initializeJmsSource("dynamicQueues/CDAP.QUEUE", 50, "org.apache.activemq.jndi.ActiveMQInitialContextFactory",
                        "vm://localhost?broker.persistent=false");

    jmsSource.setSessionAcknowledgeMode(sessionAckMode);
    jmsSource.initialize(new MockRealtimeContext());

    jmsProvider = jmsSource.getJmsProvider();
    ConnectionFactory connectionFactory = jmsProvider.getConnectionFactory();
    QueueConnection queueConn = null;
    try {
      queueConn = (QueueConnection) connectionFactory.createConnection();
      Queue queueDestination = (Queue) jmsProvider.getDestination();

      // Let's start the Connection
      queueConn.start();
      sendMessage(queueConn, queueDestination, "Queue with JNDI Provider:" + queueDestination.getQueueName());

      // Verify if it is valid
      verifyEmittedText(jmsSource);

    } finally {
      if (queueConn != null) {
        try {
          queueConn.close();
        } catch (JMSException e) {
          LOG.error("Exception when closing Queue connection.");
        }
      }
    }
  }

  // Helper method to start sending message to destination
  private void sendMessage(Connection connection, Destination destination, String destType) throws JMSException {
    Session session = connection.createSession(false, sessionAckMode);
    MessageProducer producer = session.createProducer(destination);
    TextMessage msg = session.createTextMessage();
    originalMessage = "Hello World to destination: " + destination.getClass().getName();
    msg.setText(originalMessage);
    LOG.info("Sending Message: " + msg.getText());
    producer.send(msg);

    System.out.println("Sending Message to " + destType + " destination with text: " + msg.getText());
  }

  // Helper method to verify
  private void verifyEmittedText(JmsSource source) {
    // Lets verify from JMS source
    MockEmitter emitter = new MockEmitter();
    SourceState sourceState = new SourceState();
    source.poll(emitter, sourceState);

    for (StructuredRecord val : emitter.getCurrentValues()) {
      String message = val.get(JmsSource.MESSAGE);
      Assert.assertEquals(originalMessage, message);

      System.out.println("Getting JMS Message in emitter with value: " + val.get(JmsSource.MESSAGE));
    }
  }

  private void initializeJmsSource(String destination, int messageReceive, String initialContextFactory,
                                   String providerUrl) {
    jmsSource = new JmsSource(new JmsPluginConfig(destination, initialContextFactory, providerUrl, messageReceive,
                                                  JmsSource.DEFAULT_CONNECTION_FACTORY, null, null, null));
  }

  /**
   * Helper class to emit JMS message to next stage
   */
  private static class MockEmitter implements Emitter<StructuredRecord> {
    private List<StructuredRecord> currentValues = Lists.newLinkedList();

    /**
     * Emit objects to the next stage.
     *
     * @param value data object.
     */
    @Override
    public void emit(StructuredRecord value) {
      currentValues.add(value);
    }

    /**
     * we skip the errors
     * @param value the object to emit
     */
    @Override
    public void emitError(InvalidEntry<StructuredRecord> value) {
      //no-op
    }

    List<StructuredRecord> getCurrentValues() {
      return currentValues;
    }
  }
}
