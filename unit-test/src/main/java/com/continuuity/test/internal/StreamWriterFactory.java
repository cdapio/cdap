package com.continuuity.test.internal;

import com.continuuity.common.queue.QueueName;
import com.continuuity.test.StreamWriter;
import com.google.inject.assistedinject.Assisted;

/**
 * This interface is using Guice assisted inject to create {@link com.continuuity.test.StreamWriter}.
 */
public interface StreamWriterFactory {

  StreamWriter create(QueueName queueName, @Assisted("accountId") String accountId, @Assisted("applicationId") String
    applicationId);
}
