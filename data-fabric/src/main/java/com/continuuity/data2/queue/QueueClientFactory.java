/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.queue;

import com.continuuity.common.queue.QueueName;

/**
 * Factory for creating {@link QueueClient} for different queue.
 */
public interface QueueClientFactory {

  QueueClient create(QueueName queueName);
}
