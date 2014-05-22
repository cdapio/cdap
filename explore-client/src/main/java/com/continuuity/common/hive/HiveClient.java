package com.continuuity.common.hive;

import java.io.IOException;

/**
 *
 */
public interface HiveClient {

  void sendCommand(String command) throws IOException;

}
