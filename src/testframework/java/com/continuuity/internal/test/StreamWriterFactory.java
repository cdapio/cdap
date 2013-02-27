package com.continuuity.internal.test;

import com.continuuity.app.queue.QueueName;
import com.continuuity.test.StreamWriter;
import com.google.inject.assistedinject.Assisted;

/**
 *
 */
public interface StreamWriterFactory {

  StreamWriter create(QueueName queueName,
                      @Assisted("accountId") String accountId,
                      @Assisted("applicationId") String applicationId);
}
