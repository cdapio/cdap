package com.continuuity.hive;

import com.continuuity.common.hive.HiveClient;
import com.google.common.collect.ImmutableList;
import org.apache.hive.beeline.BeeLine;

/**
 *
 */
public class WrappedHiveClient implements HiveClient {

  @Override
  public void sendCommand(String command) {

    BeeLine beeline = new BeeLine();
    beeline.runCommands(ImmutableList.of(command));
  }
}
