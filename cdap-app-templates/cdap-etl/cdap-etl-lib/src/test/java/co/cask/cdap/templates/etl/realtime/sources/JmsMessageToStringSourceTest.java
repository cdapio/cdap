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

import co.cask.cdap.api.Resources;
import co.cask.cdap.templates.etl.api.Property;
import co.cask.cdap.templates.etl.api.ValueEmitter;
import co.cask.cdap.templates.etl.api.realtime.RealtimeConfigurer;
import co.cask.cdap.templates.etl.api.realtime.RealtimeSpecification;
import co.cask.cdap.templates.etl.api.realtime.SourceContext;
import co.cask.cdap.templates.etl.realtime.jms.JmsProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
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
import javax.naming.NamingException;

/**
 * Unit test for JMS ETL realtime source
 */
public class JmsMessageToStringSourceTest {
  private static final Logger LOG = LoggerFactory.getLogger(JmsMessageToStringSourceTest.class);

  private final int sessionAckMode = Session.DUPS_OK_ACKNOWLEDGE;

  private JmsSource jmsSource;
  private JmsProvider jmsProvider;

  @Before
  public void beforeTest() {
    jmsSource = new JmsSource();

    jmsSource.configure(new RealtimeConfigurer() {
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

      @Override
      public void setResources(Resources resources) {
        // no-op
      }
    });

    jmsSource.initialize(new SourceContext() {
      @Override
      public RealtimeSpecification getSpecification() {
        return null;
      }

      @Override
      public int getInstanceId() {
        return 0;
      }

      @Override
      public int getInstanceCount() {
        return 0;
      }

      @Override
      public Map<String, String> getRuntimeArguments() {
        return null;
      }
    });
  }

  @After
  public void afterTest() {
    if(jmsSource != null) {
      jmsSource.destroy();
    }
  }

  @Test
  public void testSimpleQueueMessages() throws JMSException, NamingException {
    jmsProvider = new MockJmsProvider("dynamicQueues/CDAP.QUEUE");
    jmsSource.setJmsProvider(jmsProvider);
    jmsSource.setSessionAcknowledgeMode(sessionAckMode);

    ConnectionFactory connectionFactory = jmsProvider.getConnectionFactory();
    QueueConnection queueConn = (QueueConnection) connectionFactory.createConnection();
    Queue queueDestination = (Queue) jmsProvider.getDestination();

    // Let's start the Connection
    queueConn.start();
    sendMessage(queueConn, queueDestination, "Queue:" + queueDestination.getQueueName());

    // TODO Lets verify from JMS source
    //jmsSource.poll()

  }

  @Test
  public void testSimpleTopicMessages() throws NamingException, JMSException {
    jmsProvider = new MockJmsProvider("dynamicTopics/CDAP.TOPIC");
    jmsSource.setJmsProvider(jmsProvider);

    // TODO Add test for Topic
    jmsSource.setSessionAcknowledgeMode(sessionAckMode);

    ConnectionFactory connectionFactory = jmsProvider.getConnectionFactory();
    TopicConnection topicConn = (TopicConnection) connectionFactory.createConnection();
    Topic topicDestination = (Topic) jmsProvider.getDestination();

    // Let's start the Connection
    topicConn.start();
    sendMessage(topicConn, topicDestination, "Topic:" + topicDestination.getTopicName());

    // TODO Lets verify from JMS source
    //jmsSource.poll()

  }

  // Helper method to start sending message to destination
  public void sendMessage(Connection connection, Destination destination, String destType) throws JMSException {
    Session session = connection.createSession(false, sessionAckMode);
    MessageProducer producer = session.createProducer(destination);
    TextMessage msg = session.createTextMessage();
    msg.setText("Hello World to destination: " + destination.getClass().getName());
    LOG.info("Sending Message: " + msg.getText());
    producer.send(msg);

    System.out.println("Sending Message to " + destType + " destination with text: " + msg.getText());
  }


  /**
   * Helper class to emit JMS message to next stage
   */
  public static class MockValueEmitter implements ValueEmitter<String> {

    /**
     * Emit objects to the next stage.
     *
     * @param value data object.
     */
    @Override
    public void emit(String value) {

    }

    /**
     * Emit a key, value pair.
     *
     * @param key   key object
     * @param value value object
     */
    @Override
    public void emit(Void key, String value) {

    }
  }
}
