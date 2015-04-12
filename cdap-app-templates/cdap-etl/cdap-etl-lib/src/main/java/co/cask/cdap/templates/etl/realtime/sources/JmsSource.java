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

import co.cask.cdap.templates.etl.api.ValueEmitter;
import co.cask.cdap.templates.etl.api.realtime.RealtimeConfigurer;
import co.cask.cdap.templates.etl.api.realtime.RealtimeSource;
import co.cask.cdap.templates.etl.api.realtime.SourceContext;
import co.cask.cdap.templates.etl.api.realtime.SourceState;
import co.cask.cdap.templates.etl.realtime.jms.JmsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import javax.annotation.Nullable;
import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.TextMessage;

/**
 * <p>
 * Implementation of CDAP {@link RealtimeSource} that listen to external JMS producer by managing internal
 * JMS Consumer and send the message as String to the CDAP ETL Template flow via {@link ValueEmitter}
 * </p>
 */
public class JmsSource extends RealtimeSource<String> implements MessageListener {
  private static final Logger LOG = LoggerFactory.getLogger(JmsSource.class);

  // TODO Need option to add Max size of the internal queue
  private final BlockingQueue<Message> messageQueue = new LinkedBlockingQueue<Message>();

  private int jmsAcknowledgeMode = Session.AUTO_ACKNOWLEDGE;
  private JmsProvider jmsProvider;

  private transient Connection connection;
  private transient Session session;

  /**
   * Configure the JMS Source.
   *
   * @param configurer {@link RealtimeConfigurer}
   */
  @Override
  public void configure(RealtimeConfigurer configurer) {
    configurer.setName("JMS Realtime Source");
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
      MessageConsumer consumer = session.createConsumer(destination);
      consumer.setMessageListener(this);
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
  public SourceState poll(ValueEmitter<String> writer, SourceState currentState) {
    // Try to get message from Queue
    Message message = messageQueue.poll();
    if (message == null) {
      return currentState;
    }

    try {
      if (message instanceof TextMessage) {
        TextMessage textMessage = (TextMessage) message;
        String text = textMessage.getText();
        LOG.trace("Process JMS TextMessage : " + text);
        writer.emit(text);
      } else if (message instanceof BytesMessage) {
        BytesMessage bytesMessage = (BytesMessage) message;
        int bodyLength = (int) bytesMessage.getBodyLength();
        byte[] data = new byte[bodyLength];
        int bytesRead = bytesMessage.readBytes(data);
        if (bytesRead != bodyLength) {
          LOG.warn("Number of bytes read {} not same as expected {}", bytesRead, bodyLength);
        }
        writer.emit(new String(data));
      } else {
        // Different kind of messages, just get String for now
        // TODO Process different kind of JMS messages
        String text = message.toString();
        LOG.trace("Processing JMS message : " + text);
        writer.emit(text);
      }
    }  catch (JMSException e) {
      LOG.error("Unable to read text from a JMS Message.");
      return currentState;
    }

    return new SourceState(currentState.getState());
  }

  @Override
  public void destroy() {
    try {
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
   * <p>
   * The {@link javax.jms.MessageListener} implementation that will store the messages to be processed by next poll
   * to this {@link JmsSource}
   * </p>
   */
  @Override
  public void onMessage(Message message) {
    String messageID = "";
    try {
      messageID = message.getJMSMessageID();
    } catch (JMSException e) {
      LOG.warn("Encountered exception when trying to get message ID for JMS message.");
    }

    LOG.trace("Attempt to add message: {}", messageID);

    messageQueue.add(message);

    LOG.trace("Success adding message: {}", messageID);
  }
}
