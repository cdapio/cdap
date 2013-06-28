package com.continuuity.performance.application;

import com.continuuity.app.queue.QueueName;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.test.app.StreamWriter;
import com.continuuity.test.app.StreamWriterFactory;

/**
 * This interface is using Guice assisted inject to create
 * {@link com.continuuity.performance.gateway.stream.MultiThreadedStreamWriter}.
 */
public interface BenchmarkStreamWriterFactory extends StreamWriterFactory {
  StreamWriter create(CConfiguration config, QueueName queueName);
}