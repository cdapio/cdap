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

package co.cask.cdap.template.etl.realtime.jms;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Hashtable;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

/**
 * <p>
 *   This is the implementation of {@link JmsProvider} that tries to get {@link ConnectionFactory} through JNDI.
 *
 *   The {@link JmsProvider} implementation requires destination name and {@link Hashtable} of JNDI context that
 *   should contains all the Context environment needed to create JMS {@link ConnectionFactory} :
 *   <ul>
 *     <li>java.naming.factory.initial</li>
 *     <li>java.naming.provider.url</li>
 *   </ul>
 * </p>
 */
public class JndiBasedJmsProvider implements JmsProvider {
  private static final Logger LOG = LoggerFactory.getLogger(JndiBasedJmsProvider.class);

  private final ConnectionFactory connectionFactory;
  private final Destination destination;

  public JndiBasedJmsProvider(Hashtable<String, String> initialContextEnv, final String destinationName,
                              final String connectionFactoryName) {
    if (initialContextEnv == null) {
      throw new IllegalArgumentException("Cannot pass null initial context.");
    }

    if (destinationName == null || destinationName.trim().isEmpty()) {
      throw new IllegalArgumentException("Cannot pass null or empty destination name.");
    }

    if (connectionFactoryName == null || connectionFactoryName.trim().isEmpty()) {
      throw new IllegalArgumentException("Cannot pass null or empty connection factory name.");
    }

    Context jndiContext = null;
    try {
      jndiContext = new InitialContext(initialContextEnv);
    } catch (NamingException e) {
      LOG.error("Exception when creating initial context for connection factory.", e);
      throw new RuntimeException(e);
    }

    // Lets get the ConnectionFactory
    try {
      connectionFactory = (ConnectionFactory) jndiContext.lookup(connectionFactoryName);
      destination = (Destination) jndiContext.lookup(destinationName);
    } catch (NamingException e) {
      LOG.error("Exception when trying to do JNDI API lookup failed.", e);
      throw new RuntimeException(e);
    }
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
