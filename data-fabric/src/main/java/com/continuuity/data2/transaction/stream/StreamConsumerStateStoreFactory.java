/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.stream;

import java.io.IOException;

/**
 *
 */
public interface StreamConsumerStateStoreFactory {

  StreamConsumerStateStore create(StreamConfig streamConfig) throws IOException;
}
