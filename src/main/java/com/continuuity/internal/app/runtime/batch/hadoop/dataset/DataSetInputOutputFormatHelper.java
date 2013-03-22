package com.continuuity.internal.app.runtime.batch.hadoop.dataset;

import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.batch.Split;
import com.continuuity.internal.app.runtime.batch.hadoop.BasicHadoopMapReduceJobContext;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;

import java.util.List;
import java.util.Map;

public final class DataSetInputOutputFormatHelper {
  private static final String HCONF_ATTR_RUN_ID = "hconf.program.run.id";

  public static DataSet getDataSet(final Configuration conf, DataSetSpecification spec) {
    BasicHadoopMapReduceJobContext context = getContext(getRunId(conf));
    return context.getDataSet(spec.getName());
  }

  public static List<Split> getInput(final Configuration conf) {
    BasicHadoopMapReduceJobContext context = getContext(getRunId(conf));
    return context.getInputDataSelection();
  }

  public static void writeRunId(Configuration dest, String runId) {
    dest.set(HCONF_ATTR_RUN_ID, runId);
  }

  private static String getRunId(Configuration conf) {
    return conf.get(HCONF_ATTR_RUN_ID);
  }

  // todo: this is a hack for in-single-JVM running
  private static Map<String, BasicHadoopMapReduceJobContext> contextMap = Maps.newHashMap();

  private static synchronized BasicHadoopMapReduceJobContext getContext(String runId) {
    return contextMap.get(runId);
  }

  public static synchronized void add(String runId, BasicHadoopMapReduceJobContext context) {
    contextMap.put(runId, context);
  }

  public static synchronized void remove(String runId) {
    contextMap.remove(runId);
  }
}
