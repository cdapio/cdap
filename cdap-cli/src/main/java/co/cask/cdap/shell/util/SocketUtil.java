/*
 * Copyright © 2012-2014 Cask Data, Inc.
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

package co.cask.cdap.shell.util;

import java.io.IOException;
import java.net.Socket;

/**
 * Utilites for using {@link Socket}.
 */
public class SocketUtil {

  public static boolean isAvailable(String hostnameOrIp, int port) {
    Socket socket = null;
    boolean reachable = false;
    try {
      socket = new Socket(hostnameOrIp, port);
      return true;
    } catch (Exception e) {
      // NO-OP
    } finally {
      if (socket != null) {
        try {
          socket.close();
        } catch (IOException e) {
          // NO-OP
        }
      }
    }

    return false;
  }

}
