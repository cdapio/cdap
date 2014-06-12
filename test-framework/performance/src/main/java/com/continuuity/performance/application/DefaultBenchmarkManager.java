/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.performance.application;

import com.continuuity.app.ApplicationSpecification;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.queue.QueueName;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.data2.dataset2.DatasetFramework;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.gateway.handlers.AppFabricHttpHandler;
import com.continuuity.performance.gateway.stream.MultiThreadedStreamWriter;
import com.continuuity.test.StreamWriter;
import com.continuuity.test.internal.DefaultApplicationManager;
import com.continuuity.test.internal.ProcedureClientFactory;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**
 * Default Benchmark Context.
 */
public class DefaultBenchmarkManager extends DefaultApplicationManager {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultBenchmarkManager.class);

  private final BenchmarkStreamWriterFactory benchmarkStreamWriterFactory;
  private final Set<MultiThreadedStreamWriter> streamWriters;

  @Inject
  public DefaultBenchmarkManager(LocationFactory locationFactory,
                                 DataSetAccessor dataSetAccessor,
                                 DatasetFramework datasetFramework,
                                 TransactionSystemClient txSystemClient,
                                 BenchmarkStreamWriterFactory streamWriterFactory,
                                 ProcedureClientFactory procedureClientFactory,
                                 @Assisted("accountId") String accountId,
                                 @Assisted("applicationId") String applicationId,
                                 @Assisted Location deployedJar,
                                 @Assisted ApplicationSpecification appSpec,
                                 AppFabricHttpHandler handler) {
    super(locationFactory,
          dataSetAccessor, datasetFramework, txSystemClient,
          streamWriterFactory, procedureClientFactory,
          accountId, applicationId,
          deployedJar, appSpec, handler);
    benchmarkStreamWriterFactory = streamWriterFactory;
    streamWriters = Sets.newHashSet();
  }

  @Override
  public StreamWriter getStreamWriter(String streamName) {
    QueueName queueName = QueueName.fromStream(streamName);
    StreamWriter streamWriter = benchmarkStreamWriterFactory.create(CConfiguration.create(), queueName);
    streamWriters.add((MultiThreadedStreamWriter) streamWriter);
    return streamWriter;
  }

  public void stopAll() {
    super.stopAll();
    LOG.debug("Stopped all flowlets and procedures.");
    for (MultiThreadedStreamWriter streamWriter : streamWriters) {
      streamWriter.shutdown();
    }
    LOG.debug("Stopped all stream writers.");
  }
}
