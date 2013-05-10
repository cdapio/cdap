package com.continuuity.performance.util;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

/**
 * Tool to upload metrics to Mensa.
 */
public class MensaUtils {
  public static String buildMetric(String metric, String timestamp, String value, String benchmark, String operation,
                                   String threadCount, String extraTags) {
    StringBuilder builder = new StringBuilder();
    builder.append("put");
    builder.append(" ");
    builder.append(metric);
    builder.append(" ");
    builder.append(timestamp);
    builder.append(" ");
    builder.append(value);
    // add tags to metric
    builder.append(" benchmark=");
    builder.append(benchmark);
    builder.append(" operation=");
    builder.append(operation);
    builder.append(" threadCount=");
    builder.append(threadCount);
    if (extraTags != null && extraTags.length() != 0) {
      builder.append(" ");
      builder.append(extraTags);
    }
    return builder.toString();
  }

  public static void uploadMetric(String host, int port, String metric) throws IOException {
    DataOutputStream dos = null;
    Socket socket = null;
    try {
      socket = new Socket(host, port);
      dos = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
      metric = metric + "\n";
      dos.writeBytes(metric);
      dos.flush();
    } finally {
      if (dos != null) {
        try { dos.close();
        } catch (IOException e) {
        }
      }
      if (socket != null) {
        try { socket.close();
        } catch (IOException e) {
        }
      }
    }
  }
}
