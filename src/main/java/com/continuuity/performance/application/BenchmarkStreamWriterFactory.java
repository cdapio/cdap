package com.continuuity.performance.application;

import com.continuuity.app.queue.QueueName;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.test.StreamWriter;
import com.google.inject.assistedinject.Assisted;

/**
 * This interface is using Guice assisted inject to create
 * {@link GatewayStreamWriter}.
 */
public interface BenchmarkStreamWriterFactory {

  StreamWriter create(CConfiguration config,
                      QueueName queueName,
                      @Assisted("accountId") String accountId,
                      @Assisted("applicationId") String applicationId);
}
