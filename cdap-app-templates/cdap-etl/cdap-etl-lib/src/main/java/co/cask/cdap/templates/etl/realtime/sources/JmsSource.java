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

import javax.annotation.Nullable;
import javax.jms.Connection;
import javax.jms.Session;

/**
 * <p>
 * Implementation of CDAP {@link RealtimeSource} that listen to external JMS producer and send the message
 * as String to the CDAP ETL Template.
 * </p>
 */
public class JmsSource extends RealtimeSource<String> {
  private static final Logger LOG = LoggerFactory.getLogger(JmsSource.class);

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

    // TODO Bootstrap the JMS consumer

  }

  @Nullable
  @Override
  public SourceState poll(ValueEmitter<String> writer, SourceState currentState) {
    // TODO code this shit

    return null;
  }

  @Override
  public void destroy() {
    try {
      if(session != null) {
        session.close();
      }
      if(connection != null) {
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
   * <li>javax.jms.Session.AUTO_ACKNOWLEDGE</li>
   * <li>javax.jms.Session.CLIENT_ACKNOWLEDGE</li>
   * <li>javax.jms.Session.DUPS_OK_ACKNOWLEDGE</li>
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
    this.jmsAcknowledgeMode = mode;
  }

  /**
   * Set the {@link JmsProvider} to be used by the source.
   * implementation that this Spout will use to connect to
   * a JMS <code>javax.jms.Desination</code>
   *
   * @param provider the instance of {@link JmsProvider}
   */
  public void setJmsProvider(JmsProvider provider){
    jmsProvider = provider;
  }
}
