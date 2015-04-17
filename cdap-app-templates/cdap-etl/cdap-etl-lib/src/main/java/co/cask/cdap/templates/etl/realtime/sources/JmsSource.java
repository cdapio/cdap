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
import co.cask.cdap.templates.etl.api.StageConfigurer;
import co.cask.cdap.templates.etl.api.realtime.RealtimeSource;
import co.cask.cdap.templates.etl.api.realtime.SourceContext;
import co.cask.cdap.templates.etl.api.realtime.SourceState;
import co.cask.cdap.templates.etl.realtime.jms.JmsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  private static final long JMS_CONSUMER_TIMEOUT_MS = 30000;
  private static final String CDAP_JMS_SOURCE_NAME = "JMS Realtime Source";

  private int jmsAcknowledgeMode = Session.AUTO_ACKNOWLEDGE;
  private JmsProvider jmsProvider;

  private transient Connection connection;
  private transient Session session;
  private MessageConsumer consumer;

  /**
   * Configure the JMS Source.
   *
   * @param configurer {@link StageConfigurer}
   */
  @Override
  public void configure(StageConfigurer configurer) {
    configurer.setName(CDAP_JMS_SOURCE_NAME);
    configurer.setDescription("CDAP JMS Realtime Source");
  }

  /**
   * Initialize the Source.
   *
   * @param context {@link SourceContext}
   */
  public void initialize(SourceContext context) {
    super.initialize(context);

    // Bootstrap the JMS consumer
    initializeJMSConnection();
  }

  /**
   * Helper method to initialize the JMS Connection to start listening messages.
   */
  private void initializeJMSConnection() {
    if (jmsProvider == null) {
      throw new IllegalStateException("Could not have null JMSProvider for JMS Source. " +
                                         "Please set the right JMSProvider");
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
    try {
      message = consumer.receive(JMS_CONSUMER_TIMEOUT_MS);
    } catch (JMSException e) {
      LOG.warn("Exception when trying to receive message from JMS consumer: {}", CDAP_JMS_SOURCE_NAME);
    }
    if (message == null) {
      return currentState;
    }

    String text;
    try {
      if (message instanceof TextMessage) {
        TextMessage textMessage = (TextMessage) message;
        text = textMessage.getText();
        LOG.trace("Process JMS TextMessage : ", text);
      } else if (message instanceof BytesMessage) {
        BytesMessage bytesMessage = (BytesMessage) message;
        int bodyLength = (int) bytesMessage.getBodyLength();
        byte[] data = new byte[bodyLength];
        int bytesRead = bytesMessage.readBytes(data);
        if (bytesRead != bodyLength) {
          LOG.warn("Number of bytes read {} not same as expected {}", bytesRead, bodyLength);
        }
        text = new String(data).intern();
        LOG.trace("Processing JMS ByteMessage : {}", text);
      } else {
        // Different kind of messages, just get String for now
        // TODO Process different kind of JMS messages
        text = message.toString();
        LOG.trace("Processing JMS message : ", text);
      }
    }  catch (JMSException e) {
      LOG.error("Unable to read text from a JMS Message.");
      return currentState;
    }

    writer.emit(text);

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
}
