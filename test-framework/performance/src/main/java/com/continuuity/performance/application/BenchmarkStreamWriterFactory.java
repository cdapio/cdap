/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.performance.application;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.queue.QueueName;
import com.continuuity.test.StreamWriter;
import com.continuuity.test.internal.StreamWriterFactory;

/**
 * This interface is using Guice assisted inject to create
 * {@link com.continuuity.performance.gateway.stream.MultiThreadedStreamWriter}.
 */
public interface BenchmarkStreamWriterFactory extends StreamWriterFactory {
  StreamWriter create(CConfiguration config, QueueName queueName);
}
