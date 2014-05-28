package com.continuuity.hive.client;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Interface that serves as a Hive Client that sends commands to a Hive server. The underlying implementations take
 * care of discovering the Hive server.
 */
public interface HiveClient {

  /**
   * Sends a command to a Hive server.
   * @param command String command to send to Hive
   * @param out will contain the standard output of the command
   * @param err will contain the standard error of the command
   * @throws IOException
   */
  void sendCommand(String command, OutputStream out, OutputStream err) throws IOException;

}
