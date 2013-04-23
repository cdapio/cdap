package com.continuuity.internal.app.runtime.batch.inmemory;

import com.continuuity.internal.app.runtime.batch.BasicMapReduceContext;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;

import java.util.Map;

public final class MapReduceContextAccessor {
  private static final String HCONF_ATTR_RUN_ID = "hconf.program.run.id";

  public static BasicMapReduceContext getContext(final Configuration conf) {
    return getContext(getRunId(conf));
  }

  public static void setRunId(Configuration dest, String runId) {
    dest.set(HCONF_ATTR_RUN_ID, runId);
  }

  private static String getRunId(Configuration conf) {
    return conf.get(HCONF_ATTR_RUN_ID);
  }

  private static Map<String, BasicMapReduceContext> contextMap = Maps.newHashMap();

  private static synchronized BasicMapReduceContext getContext(String runId) {
    return contextMap.get(runId);
  }

  public static synchronized void put(String runId, BasicMapReduceContext context) {
    contextMap.put(runId, context);
  }

  public static synchronized void remove(String runId) {
    contextMap.remove(runId);
  }
}
