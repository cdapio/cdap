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

package co.cask.cdap.template.etl.realtime.source;

import co.cask.cdap.template.etl.realtime.jms.JmsProvider;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

/**
 * This class is used to provide mock JMS ConnectionFactory for testing JMS realtime template source.
 */
public class MockJmsProvider implements JmsProvider {
  public static final String CONNECTION_URL = "vm://localhost?broker.persistent=false&broker.useJmx=false";

  private ConnectionFactory connectionFactory;
  private Destination destination;

  public MockJmsProvider(String destinationName) throws NamingException {
    connectionFactory =  new ActiveMQConnectionFactory(CONNECTION_URL);

    // Set JNDI Context
    Context jndiContext = new InitialContext();
    destination = (Destination) jndiContext.lookup(destinationName);
  }

  @Override
  public ConnectionFactory getConnectionFactory() {
    return connectionFactory;
  }

  @Override
  public Destination getDestination() {
    return destination;
  }
}
