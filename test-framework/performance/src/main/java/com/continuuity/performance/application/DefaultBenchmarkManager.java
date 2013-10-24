/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.performance.application;

import com.continuuity.api.ApplicationSpecification;
import com.continuuity.app.services.AppFabricService;
import com.continuuity.app.services.AuthToken;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.queue.QueueName;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.performance.gateway.stream.MultiThreadedStreamWriter;
import com.continuuity.test.StreamWriter;
import com.continuuity.test.internal.DefaultApplicationManager;
import com.continuuity.test.internal.ProcedureClientFactory;
import com.continuuity.weave.filesystem.Location;
import com.continuuity.weave.filesystem.LocationFactory;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
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
                                 TransactionSystemClient txSystemClient,
                                 BenchmarkStreamWriterFactory streamWriterFactory,
                                 ProcedureClientFactory procedureClientFactory,
                                 @Assisted AuthToken token,
                                 @Assisted("accountId") String accountId,
                                 @Assisted("applicationId") String applicationId,
                                 @Assisted AppFabricService.Iface appFabricServer,
                                 @Assisted Location deployedJar,
                                 @Assisted ApplicationSpecification appSpec) {
    super(locationFactory,
          dataSetAccessor, txSystemClient,
          streamWriterFactory, procedureClientFactory,
          token, accountId, applicationId,
          appFabricServer, deployedJar, appSpec);
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
