package com.continuuity.hive.client;

import java.io.IOException;

/**
 *
 */
public interface HiveClient {

  void sendCommand(String command) throws IOException;

}
