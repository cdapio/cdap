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

package co.cask.cdap.etl.realtime.jms;

import co.cask.cdap.etl.api.realtime.RealtimeSource;

import javax.jms.ConnectionFactory;
import javax.jms.Destination;

/**
 * Define an interface to get <code>Destination</code> JMS objects and <code>ConnectionFactory</code> to manage
 * a topic/queue connection over the course of a source lifecycle.
 */
public interface JmsProvider {

  /**
   * Provides the JMS <code>ConnectionFactory</code>
   * @return the instance of JMS {@link ConnectionFactory}
   */
  ConnectionFactory getConnectionFactory();

  /**
   * Provides the <code>Destination</code> (topic or queue) from which the {@link RealtimeSource} will get
   * data from.
   * @return instance of JMS {@link Destination}
   */
  Destination getDestination();
}
