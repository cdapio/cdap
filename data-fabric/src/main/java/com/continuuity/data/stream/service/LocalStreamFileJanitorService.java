/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data.stream.service;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.data.stream.StreamFileJanitor;
import com.google.common.util.concurrent.AbstractService;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Performs cleanup of stream files regularly with a single thread executor.
 */
@Singleton
public final class LocalStreamFileJanitorService extends AbstractService implements StreamFileJanitorService {

  private static final Logger LOG = LoggerFactory.getLogger(LocalStreamFileJanitorService.class);

  private final StreamFileJanitor janitor;
  private final long cleanupPeriod;
  private ScheduledExecutorService executor;

  @Inject
  public LocalStreamFileJanitorService(StreamFileJanitor janitor, CConfiguration cConf) {
    this.janitor = janitor;
    this.cleanupPeriod = cConf.getLong(Constants.Stream.FILE_CLEANUP_PERIOD);
  }

  @Override
  protected void doStart() {
    executor = new ScheduledThreadPoolExecutor(1, Threads.createDaemonThreadFactory("stream-cleanup")) {
      @Override
      protected void terminated() {
        notifyStopped();
      }
    };

    // Always run the cleanup when it starts
    executor.submit(new Runnable() {

      @Override
      public void run() {
        LOG.debug("Execute stream file cleanup.");

        try {
          janitor.cleanAll();
          LOG.debug("Completed stream file cleanup.");
        } catch (Throwable e) {
          LOG.error("Failed to cleanup stream file.", e);
        } finally {
          // Compute the next cleanup time. It is aligned to work clock based on the period.
          long now = System.currentTimeMillis();
          long delay = (now / cleanupPeriod + 1) * cleanupPeriod - now;

          if (delay <= 0) {
            executor.submit(this);
          } else {
            LOG.debug("Schedule stream file cleanup in {} ms", delay);
            executor.schedule(this, delay, TimeUnit.MILLISECONDS);
          }
        }
      }
    });
    notifyStarted();
  }

  @Override
  protected void doStop() {
    executor.shutdownNow();
  }
}
