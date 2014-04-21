package com.continuuity.internal.app.scheduler;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
*
*/
public class LogPrintingJob implements Job {

  private static final Logger LOG = LoggerFactory.getLogger(LogPrintingJob.class);

  @Override
  public void execute(JobExecutionContext context) throws JobExecutionException {
    try {
      LOG.info("Received Trigger at {}", context.getScheduledFireTime().toString());

      JobDataMap map = context.getMergedJobDataMap();
      String[] keys = map.getKeys();

      Preconditions.checkArgument(keys != null);
      Preconditions.checkArgument(keys.length > 0);
      LOG.info("Number of parameters {}", keys.length);
      for (String key : keys) {
        LOG.info("Parameter key: {}, value: {}", key, map.get(key));
      }
    } catch (Throwable e) {
      throw Throwables.propagate(e);
    }
    throw new JobExecutionException("excepion");
  }
}
