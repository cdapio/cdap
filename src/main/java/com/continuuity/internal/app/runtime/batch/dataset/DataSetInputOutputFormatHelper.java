package com.continuuity.internal.app.runtime.batch.dataset;

import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.batch.Split;
import com.continuuity.common.logging.LoggingContextAccessor;
import com.continuuity.internal.app.runtime.batch.BasicMapReduceContext;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;

import java.util.List;
import java.util.Map;

public final class DataSetInputOutputFormatHelper {
  private static final String HCONF_ATTR_RUN_ID = "hconf.program.run.id";

  public static DataSet getDataSet(final Configuration conf, DataSetSpecification spec) {
    BasicMapReduceContext context = getContext(getRunId(conf));
    // hack: making sure logging context is set for the thread that accesses the runtime context
    LoggingContextAccessor.setLoggingContext(context.getLoggingContext());
    return context.getDataSet(spec.getName());
  }

  public static List<Split> getInput(final Configuration conf) {
    BasicMapReduceContext context = getContext(getRunId(conf));
    return context.getInputDataSelection();
  }

  public static void writeRunId(Configuration dest, String runId) {
    dest.set(HCONF_ATTR_RUN_ID, runId);
  }

  private static String getRunId(Configuration conf) {
    return conf.get(HCONF_ATTR_RUN_ID);
  }

  private static Map<String, BasicMapReduceContext> contextMap = Maps.newHashMap();

  private static synchronized BasicMapReduceContext getContext(String runId) {
    return contextMap.get(runId);
  }

  public static synchronized void add(String runId, BasicMapReduceContext context) {
    contextMap.put(runId, context);
  }

  public static synchronized void remove(String runId) {
    contextMap.remove(runId);
  }
}
