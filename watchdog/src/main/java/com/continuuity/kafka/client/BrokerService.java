/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.kafka.client;

import com.google.common.util.concurrent.Service;

/**
 * Service for providing information of kafka brokers.
 */
public interface BrokerService extends Service {

  /**
   * Returns the broker information of the current leader of the given topic and partition.
   * @param topic Topic for looking up for leader.
   * @param partition Partition for looking up for leader.
   * @return A {@link BrokerInfo} containing information about the current leader, or {@code null} if
   *         current leader is unknown.
   */
  BrokerInfo getLeader(String topic, int partition);

  /**
   * Returns a live iterable that gives information for all the known brokers.
   */
  Iterable<BrokerInfo> getBrokers();

  /**
   * Returns a comma separate string of all current brokers.
   * @return A string in the format {@code host1:port1,host2:port2} or empty string if no broker has been discovered.
   */
  String getBrokerList();
}
