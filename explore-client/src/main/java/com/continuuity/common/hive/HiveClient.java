package com.continuuity.common.hive;

import com.continuuity.hive.HiveServiceNotFoundException;

import java.io.IOException;

/**
 *
 */
public interface HiveClient {

  void sendCommand(String command) throws IOException, HiveServiceNotFoundException;

}
