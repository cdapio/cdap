/*
 * Copyright © 2018 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.monitor;

import java.net.InetAddress;
import java.net.InetSocketAddress;

/**
 * A class containing information about the {@link RuntimeMonitorServer} that are used by the client.
 */
public final class RuntimeMonitorServerInfo {

  private final InetAddress hostAddress;
  private final int port;

  public RuntimeMonitorServerInfo(InetSocketAddress address) {
    this.hostAddress = address.getAddress();
    this.port = address.getPort();
  }

  /**
   * Returns the host address that the {@link RuntimeMonitorServer} is listening on.
   */
  public InetAddress getHostAddress() {
    return hostAddress;
  }

  /**
   * Returns the port that the {@link RuntimeMonitorServer} is listening on.
   */
  public int getPort() {
    return port;
  }
}
