package com.continuuity.app.logging;

import com.continuuity.common.logging.ApplicationLoggingContext;

/**
 *
 */
public class MapReduceJobLoggingContext extends ApplicationLoggingContext {

  public static final String TAG_MAP_REDUCE_JOB_ID = "mapReduceJobId";

  /**
   * Constructs the MapReduceJobLoggingContext
   * @param accountId account id
   * @param applicationId application id
   * @param mapReduceJobId mapreduce job id
   */
  public MapReduceJobLoggingContext(final String accountId, final String applicationId, final String mapReduceJobId) {
    super(accountId, applicationId);
    setSystemTag(TAG_MAP_REDUCE_JOB_ID, mapReduceJobId);
  }

  @Override
  public String getLogPartition() {
    return super.getLogPartition() + String.format(":%s", getSystemTag(TAG_MAP_REDUCE_JOB_ID));
  }

}
