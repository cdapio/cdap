/*
 * Copyright © 2019 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.monitor.proxy;

import co.cask.cdap.runtime.spi.ssh.PortForwarding;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * This interface represents provider for {@link PortForwarding}, which is used by the {@link MonitorSocksProxy}.
 */
public interface PortForwardingProvider {

  /**
   * Creates a local {@link PortForwarding} to the given server.
   *
   * @param serverAddr the {@link InetSocketAddress} of where the runtime monitor server is running
   * @param dataConsumer the {@link PortForwarding.DataConsumer} for consuming data coming from
   *                     the port forwarding channel
   * @throws IOException if failed to create the port forwarding
   * @throws IllegalStateException if the request address is not allowed to have SSH connection
   */
  PortForwarding createPortForwarding(InetSocketAddress serverAddr,
                                      PortForwarding.DataConsumer dataConsumer) throws IOException;
}
