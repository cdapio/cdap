package com.continuuity.common.utils;

import java.io.IOException;
import java.net.Socket;

public class PortDetector {

  /**
   * Utility to find a free port to use for a server.
   *
   * @return a free port number
   * @throws java.io.IOException if any socket exception occurs
   */
  public static int findFreePort() throws IOException {
    Socket socket = new Socket();
    socket.bind(null);
    int port = socket.getLocalPort();
    socket.close();
    return port;
  }
}
