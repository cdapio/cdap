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

package co.cask.cdap.templates.etl.realtime.sources;

import co.cask.cdap.templates.etl.api.Emitter;
import co.cask.cdap.templates.etl.api.Property;
import co.cask.cdap.templates.etl.api.StageConfigurer;
import co.cask.cdap.templates.etl.api.realtime.SourceState;
import co.cask.cdap.templates.etl.common.MockSourceContext;
import co.cask.cdap.templates.etl.realtime.jms.JmsProvider;
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
public class JmsMessageToStringSourceTest {
  private static final Logger LOG = LoggerFactory.getLogger(JmsMessageToStringSourceTest.class);

  private final int sessionAckMode = Session.AUTO_ACKNOWLEDGE;

  private JmsSource jmsSource;
  private JmsProvider jmsProvider;

  private String originalMessage;

  @Before
  public void beforeTest() {
    jmsSource = new JmsSource();

    jmsSource.configure(new StageConfigurer() {
      @Override
      public void setName(String name) {
        // no-op
      }

      @Override
      public void setDescription(String description) {
        // no-op
      }

      @Override
      public void addProperties(List<Property> properties) {
        // no-op
      }

      @Override
      public void addProperty(Property property) {
        // no-op
      }
    });
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
    jmsProvider = new MockJmsProvider("dynamicQueues/CDAP.QUEUE");
    jmsSource.setJmsProvider(jmsProvider);
    jmsSource.setSessionAcknowledgeMode(sessionAckMode);

    jmsSource.initialize(new MockSourceContext());

    ConnectionFactory connectionFactory = jmsProvider.getConnectionFactory();
    QueueConnection queueConn = null;
    try {
      queueConn = (QueueConnection) connectionFactory.createConnection();
      Queue queueDestination = (Queue) jmsProvider.getDestination();

      // Let's start the Connection
      queueConn.start();
      sendMessage(queueConn, queueDestination, "Queue:" + queueDestination.getQueueName());

      // Verify if it is valid
      verifyEmittedText(jmsSource, 5, 4000);

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
    jmsProvider = new MockJmsProvider("dynamicTopics/CDAP.TOPIC");
    jmsSource.setJmsProvider(jmsProvider);
    jmsSource.setSessionAcknowledgeMode(sessionAckMode);

    jmsSource.initialize(new MockSourceContext());

    ConnectionFactory connectionFactory = jmsProvider.getConnectionFactory();
    TopicConnection topicConn = null;
    try {
      topicConn = (TopicConnection) connectionFactory.createConnection();

      Topic topicDestination = (Topic) jmsProvider.getDestination();

      // Let's start the Connection
      topicConn.start();
      sendMessage(topicConn, topicDestination, "Topic:" + topicDestination.getTopicName());

      // Verify if it is valid
      verifyEmittedText(jmsSource, 5, 4000);
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
  private void verifyEmittedText(JmsSource source, int numTries, long sleepMilis) {
    // Lets verify from JMS source
    MockEmitter emitter = new MockEmitter();
    SourceState sourceState = new SourceState();
    source.poll(emitter, sourceState);

    int i = 1;
    while (i < numTries) {
      if (emitter.getCurrentValue() == null) {
        try {
          Thread.sleep(sleepMilis);
        } catch (InterruptedException e) {
          // no-op
        }
        source.poll(emitter, sourceState);
        i++;
      } else {
        break;
      }
    }
    String emitterValue = emitter.getCurrentValue();
    Assert.assertEquals(originalMessage, emitterValue);

    System.out.println("Getting JMS Message in emitter with value: " + emitterValue);
  }

  /**
   * Helper class to emit JMS message to next stage
   */
  private static class MockEmitter implements Emitter<String> {
    private String currentValue;

    /**
     * Emit objects to the next stage.
     *
     * @param value data object.
     */
    @Override
    public void emit(String value) {
      currentValue = value;
    }

    String getCurrentValue() {
      return currentValue;
    }
  }
}
