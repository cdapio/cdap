package com.continuuity.hive.client;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Interface that serves as a Hive Client that sends commands to a Hive server. The underlying implementations take
 * care of discovering the Hive server.
 */
public interface HiveClient {

  void sendCommand(String command, OutputStream out, OutputStream err) throws IOException;

}
