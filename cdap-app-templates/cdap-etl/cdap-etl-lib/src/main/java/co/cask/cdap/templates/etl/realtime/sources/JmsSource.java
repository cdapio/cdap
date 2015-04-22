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
import co.cask.cdap.templates.etl.api.realtime.RealtimeContext;
import co.cask.cdap.templates.etl.api.realtime.RealtimeSource;
import co.cask.cdap.templates.etl.api.realtime.SourceState;
import co.cask.cdap.templates.etl.realtime.jms.JmsProvider;
import co.cask.cdap.templates.etl.realtime.jms.JndiBasedJmsProvider;
import com.google.common.base.Predicate;
import com.google.common.collect.Maps;
import org.apache.hadoop.util.hash.Hash;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Hashtable;
import java.util.Map;
import javax.annotation.Nullable;
import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;

/**
 * <p>
 * Implementation of CDAP {@link RealtimeSource} that listen to external JMS producer by managing internal
 * JMS Consumer and send the message as String to the CDAP ETL Template flow via {@link Emitter}
 * </p>
 */
public class JmsSource extends RealtimeSource<String> {
  private static final Logger LOG = LoggerFactory.getLogger(JmsSource.class);

  public static final String JMS_DESTINATION_NAME = "jms.destination.name";

  private static final long JMS_CONSUMER_TIMEOUT_MS = 2000;
  private static final String CDAP_JMS_SOURCE_NAME = "JMS Realtime Source";
  private static final String JMS_MESSAGES_TO_RECEIVE = "jms.messages.receive";

  private int jmsAcknowledgeMode = Session.AUTO_ACKNOWLEDGE;
  private JmsProvider jmsProvider;

  private transient Connection connection;
  private transient Session session;
  private transient MessageConsumer consumer;

  private int messagesToReceive = 50;

  /**
   * Configure the JMS Source.
   *
   * @param configurer {@link StageConfigurer}
   */
  @Override
  public void configure(StageConfigurer configurer) {
    configurer.setName(CDAP_JMS_SOURCE_NAME);
    configurer.setDescription("CDAP JMS Realtime Source");
    configurer.addProperty(new Property("jms.messages.receive", "Number messages should be retrieved " +
      "for each poll", false));
    configurer.addProperty(new Property("jms.destination.name", "Name of the destination to get messages " +
      "from.", false));
  }

  /**
   * Initialize the Source.
   *
   * @param context {@link RealtimeContext}
   */
  public void initialize(RealtimeContext context) throws Exception {
    super.initialize(context);

    if (context.getRuntimeArguments().get(JMS_MESSAGES_TO_RECEIVE) != null) {
      messagesToReceive = Integer.valueOf(context.getRuntimeArguments().get(JMS_MESSAGES_TO_RECEIVE));
    }

    // Try get the destination name
    String destinationName = context.getRuntimeArguments().get(JMS_DESTINATION_NAME);

    // Get environment vars - this would be prefixed with java.naming.*
    final Hashtable<String, String> envVars = new Hashtable<String, String>();
    Maps.filterEntries(context.getRuntimeArguments(), new Predicate<Map.Entry<String, String>>() {
      @Override
      public boolean apply(@Nullable Map.Entry<String, String> input) {
        if (input.getKey() != null && input.getKey().startsWith("java.naming.")) {
          envVars.put(input.getKey(), input.getValue());
          return true;
        }
        return false;
      }
    });

    // Bootstrap the JMS consumer
    initializeJMSConnection(envVars, destinationName);
  }

  /**
   * Helper method to initialize the JMS Connection to start listening messages.
   */
  private void initializeJMSConnection(Hashtable<String, String> envVars, String destinationName) {
    if (jmsProvider == null) {
      if (destinationName == null) {
        throw new IllegalStateException("Could not have null JMSProvider for JMS Source. " +
                                          "Please set the right JMSProvider");
      } else {
        jmsProvider = new JndiBasedJmsProvider(envVars, destinationName);
      }
    }
    ConnectionFactory connectionFactory = jmsProvider.getConnectionFactory();

    try {
      connection = connectionFactory.createConnection();
      session = connection.createSession(false, jmsAcknowledgeMode);
      Destination destination = jmsProvider.getDestination();
      consumer = session.createConsumer(destination);
      connection.start();
    } catch (JMSException ex) {
      if (session != null) {
        try {
          session.close();
        } catch (JMSException ex1) {
          LOG.warn("Exception when closing session", ex1);
        }
      }
      if (connection != null) {
        try {
          connection.close();
        } catch (JMSException ex2) {
          LOG.warn("Exception when closing connection", ex2);
        }
      }
      throw new RuntimeException("JMSException thrown when trying to initialize connection: " + ex.getMessage(),
                                 ex);
    }
  }

  @Nullable
  @Override
  public SourceState poll(Emitter<String> writer, SourceState currentState) {
    // Try to get message from Queue
    Message message = null;

    int count = 0;
    do {
      try {
        message = consumer.receive(JMS_CONSUMER_TIMEOUT_MS);
      } catch (JMSException e) {
        LOG.warn("Exception when trying to receive message from JMS consumer: {}", CDAP_JMS_SOURCE_NAME);
      }
      if (message != null) {
        String text;
        try {
          if (message instanceof TextMessage) {
            TextMessage textMessage = (TextMessage) message;
            text = textMessage.getText();
            LOG.trace("Process JMS TextMessage : ", text);
          } else if (message instanceof BytesMessage) {
            BytesMessage bytesMessage = (BytesMessage) message;
            text = bytesMessage.readUTF();
            LOG.trace("Processing JMS ByteMessage : {}", text);
          } else {
            // Different kind of messages, just get String for now
            // TODO Process different kind of JMS messages
            text = message.toString();
            LOG.trace("Processing JMS message : ", text);
          }
        } catch (JMSException e) {
          LOG.error("Unable to read text from a JMS Message.");
          continue;
        }

        writer.emit(text);
        count++;
      }
    } while (message != null && count < messagesToReceive);

    return new SourceState(currentState.getState());
  }

  @Override
  public void destroy() {
    try {
      if (consumer != null) {
        consumer.close();
      }

      if (session != null) {
        session.close();
      }

      if (connection != null) {
        connection.close();
      }
    } catch (Exception ex) {
      throw new RuntimeException("Exception on closing JMS connection: " + ex.getMessage(), ex);
    }
  }

  /**
   * Sets the JMS Session acknowledgement mode for this source.
   * <p/>
   * Possible values:
   * <ul>
   *  <li>javax.jms.Session.AUTO_ACKNOWLEDGE</li>
   *  <li>javax.jms.Session.CLIENT_ACKNOWLEDGE</li>
   *  <li>javax.jms.Session.DUPS_OK_ACKNOWLEDGE</li>
   *  <li>javax.jms.Session.SESSION_TRANSACTED</li>
   * </ul>
   * @param mode JMS Session Acknowledgement mode
   * @throws IllegalArgumentException if the mode is not recognized.
   */
  public void setSessionAcknowledgeMode(int mode) {
    switch (mode) {
      case Session.AUTO_ACKNOWLEDGE:
      case Session.CLIENT_ACKNOWLEDGE:
      case Session.DUPS_OK_ACKNOWLEDGE:
      case Session.SESSION_TRANSACTED:
        break;
      default:
        throw new IllegalArgumentException("Unknown JMS Session acknowledge mode: " + mode);
    }
    jmsAcknowledgeMode = mode;
  }

  /**
   * Set the {@link JmsProvider} to be used by the source.
   *
   * @param provider the instance of {@link JmsProvider}
   */
  public void setJmsProvider(JmsProvider provider) {
    jmsProvider = provider;
  }

  /**
   * Return the internal {@link JmsProvider} used by the source.
   *
   * @return the instance of {@link JmsProvider} for this JMS source.
   */
  public JmsProvider getJmsProvider() {
    return jmsProvider;
  }
}
