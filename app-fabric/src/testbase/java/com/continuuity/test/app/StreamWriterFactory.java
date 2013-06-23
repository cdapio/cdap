package com.continuuity.test.app;

import com.continuuity.app.queue.QueueName;
import com.google.inject.assistedinject.Assisted;

/**
 * This interface is using Guice assisted inject to create {@link com.continuuity.test.app.StreamWriter}.
 */
public interface StreamWriterFactory {

  StreamWriter create(QueueName queueName, @Assisted("accountId") String accountId, @Assisted("applicationId") String
    applicationId);
}
