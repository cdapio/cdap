/*
 * Copyright © 2020 Cask Data, Inc.
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

import org.apache.twill.internal.zookeeper.InMemoryZKServer;

import java.net.InetAddress;
import java.net.InetSocketAddress;

public class SimpleMain {

  public static void main(String[] args) {
    InMemoryZKServer server = InMemoryZKServer.builder().build();
    server.startAndWait();
    System.out.println(server.getConnectionStr());
    InetSocketAddress addr = resolve(server.getLocalAddress());
    System.out.println(addr);

    String connStr = String.format("%s:%d", addr.getHostName(), addr.getPort());
    System.out.println(connStr);
  }

  public static InetSocketAddress resolve(InetSocketAddress bindAddress) {
    try {
      // If domain of bindAddress is not resolvable, address of bindAddress is null.
      if (bindAddress.getAddress() != null && bindAddress.getAddress().isAnyLocalAddress()) {
        return new InetSocketAddress(InetAddress.getLocalHost().getHostName(), bindAddress.getPort());
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return bindAddress;
  }
}
