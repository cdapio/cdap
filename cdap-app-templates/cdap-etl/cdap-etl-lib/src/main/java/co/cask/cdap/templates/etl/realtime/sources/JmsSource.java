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

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.templates.plugins.PluginConfig;
import co.cask.cdap.api.templates.plugins.PluginProperties;
import co.cask.cdap.templates.etl.api.Emitter;
import co.cask.cdap.templates.etl.api.PipelineConfigurer;
import co.cask.cdap.templates.etl.api.realtime.RealtimeContext;
import co.cask.cdap.templates.etl.api.realtime.RealtimeSource;
import co.cask.cdap.templates.etl.api.realtime.SourceState;
import co.cask.cdap.templates.etl.realtime.jms.JmsProvider;
import co.cask.cdap.templates.etl.realtime.jms.JndiBasedJmsProvider;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Maps;
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
import javax.naming.Context;

/**
 * <p>
 * Implementation of CDAP {@link RealtimeSource} that listen to external JMS producer by managing internal
 * JMS Consumer and send the message as String to the CDAP ETL Template flow via {@link Emitter}
 * </p>
 */
@Plugin(type = "source")
@Name("JMS")
@Description("JMS Realtime Source")
public class JmsSource extends RealtimeSource<StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(JmsSource.class);

  public static final String JAVA_NAMING_PREFIX = "java.naming";
  public static final String JMS_DESTINATION_NAME = "jms.destination.name";
  public static final String JMS_MESSAGES_TO_RECEIVE = "jms.messages.receive";
  public static final String JMS_NAMING_FACTORY_INITIAL = "jms.factory.initial";
  public static final String JMS_PROVIDER_URL = "jms.provider.url";
  public static final String JMS_CONNECTION_FACTORY_NAME = "ConnectionFactory";

  private static final long JMS_CONSUMER_TIMEOUT_MS = 2000;

  public static final String MESSAGE = "message";

  private static final Schema SCHEMA = Schema.recordOf("JMS Message",
                                                       Schema.Field.of(MESSAGE, Schema.of(Schema.Type.STRING)));

  private final JmsPluginConfig config;

  private int jmsAcknowledgeMode = Session.AUTO_ACKNOWLEDGE;
  private JmsProvider jmsProvider;

  private transient Connection connection;
  private transient Session session;
  private transient MessageConsumer consumer;

  private int messagesToReceive;

  /**
   * Default constructor
   *
   * @param config The configuration needed for the JMS source.
   */
  public JmsSource(JmsPluginConfig config) {
    this.config = config;
  }

  /**
   * Initialize the Source.
   *
   * @param context {@link RealtimeContext}
   */
  public void initialize(RealtimeContext context) throws Exception {
    super.initialize(context);
    Map<String, String> runtimeArguments = Maps.newHashMap();
    if (config.getProperties() != null) {
      runtimeArguments.putAll(config.getProperties().getProperties());
    }

    Integer configMessagesToReceive = config.messagesToReceive;
    messagesToReceive = configMessagesToReceive.intValue();

    // Get environment vars - this would be prefixed with java.naming.*
    final Hashtable<String, String> envVars = new Hashtable<String, String>();
    for (Map.Entry<String, String> entry : runtimeArguments.entrySet()) {
      if (entry.getKey() != null && entry.getKey().startsWith(JAVA_NAMING_PREFIX)) {
        envVars.put(entry.getKey(), entry.getValue());
      }
    }

    // Set initial context factory name and provider URL
    envVars.put(Context.INITIAL_CONTEXT_FACTORY, config.initialContextFactory);
    envVars.put(Context.PROVIDER_URL, config.providerUrl);

    // Bootstrap the JMS consumer
    initializeJMSConnection(envVars, config.destinationName, config.connectionFactoryName);
  }

  /**
   * Helper method to initialize the JMS Connection to start listening messages.
   */
  private void initializeJMSConnection(Hashtable<String, String> envVars, String destinationName,
                                       String connectionFactoryName) {
    if (jmsProvider == null) {
      LOG.trace("JMS provider is not set when trying to initialize JMS connection.");
      if (destinationName == null) {
        throw new IllegalStateException("Could not have null JMSProvider for JMS Source. " +
                                          "Please set the right JMSProvider");
      } else {
        LOG.trace("Using JNDI default JMS provider for destination: {}", destinationName);
        jmsProvider = new JndiBasedJmsProvider(envVars, destinationName, connectionFactoryName);
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

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    pipelineConfigurer.usePluginClass("JMSProvider", Context.INITIAL_CONTEXT_FACTORY, "jmsource.JMSProvider.Context",
                                      PluginProperties.builder().build());
  }

  @Nullable
  @Override
  public SourceState poll(Emitter<StructuredRecord> writer, SourceState currentState) {
    // Try to get message from Queue
    Message message = null;

    int count = 0;
    do {
      try {
        message = consumer.receive(JMS_CONSUMER_TIMEOUT_MS);
      } catch (JMSException e) {
        LOG.warn("Exception when trying to receive message from JMS consumer.");
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

        writer.emit(stringMessageToStructuredRecord(text));
        count++;
      }
    } while (message != null && count < messagesToReceive);

    return new SourceState(currentState.getState());
  }

  // Helper method to encode JMS String message to StructuredRecord.
  private static StructuredRecord stringMessageToStructuredRecord(String text) {
    StructuredRecord.Builder recordBuilder = StructuredRecord.builder(SCHEMA);
    recordBuilder.set(MESSAGE, text);
    return recordBuilder.build();
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

  /**
   * Config class for {@link JmsSource}.
   */
  public static class JmsPluginConfig extends PluginConfig {
    @Name(JMS_DESTINATION_NAME)
    @Description("Name of the destination to get messages")
    private String destinationName;

    @Name(JMS_MESSAGES_TO_RECEIVE)
    @Description("Max number messages should be retrieved per poll. The default value is 50.")
    @Nullable
    private Integer messagesToReceive;

    @Name(JMS_NAMING_FACTORY_INITIAL)
    @Description("The fully qualified class name of the factory class that will create an initial context. " +
      "This will be passed to JNDI initial context as " + Context.INITIAL_CONTEXT_FACTORY)
    private String initialContextFactory;

    @Name(JMS_PROVIDER_URL)
    @Description("This property contains information for the service provider URL to use. " +
      "This will be passed to JNDI initial context as " + Context.PROVIDER_URL)
    private String providerUrl;

    @Name(JMS_CONNECTION_FACTORY_NAME)
    @Description("The name of the connection factory from the JNDI. The default will be ConnectionFactory.")
    @Nullable
    private String connectionFactoryName;

    public JmsPluginConfig() {
      messagesToReceive = 50;
      connectionFactoryName = "ConnectionFactory";
    }

    public JmsPluginConfig(String destinationName, @Nullable Integer messagesToReceive, String initialContextFactory,
                           String providerUrl, @Nullable String connectionFactoryName) {
      this.destinationName = destinationName;
      if (messagesToReceive != null) {
        this.messagesToReceive = messagesToReceive;
      } else {
        this.messagesToReceive = 50;
      }
      this.initialContextFactory = initialContextFactory;
      this.providerUrl = providerUrl;
      if (connectionFactoryName != null) {
        this.connectionFactoryName = connectionFactoryName;
      } else {
        this.connectionFactoryName = JMS_CONNECTION_FACTORY_NAME;
      }
    }
  }
}
