package com.continuuity.common.hive;

import java.io.IOException;
import java.io.OutputStream;

/**
 *
 */
public interface HiveClient {

  void sendCommand(String command, OutputStream out, OutputStream err) throws IOException;

}
