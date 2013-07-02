/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.kafka.client;

import com.google.common.util.concurrent.Service;

/**
 * Service for providing information of kafka brokers.
 */
public interface BrokerService extends Service {

  BrokerInfo getLeader(String topic, int partition);
}
