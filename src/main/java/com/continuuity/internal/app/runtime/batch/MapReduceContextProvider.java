package com.continuuity.internal.app.runtime.batch;

import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.batch.SimpleSplit;
import com.continuuity.api.data.batch.Split;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.internal.app.runtime.batch.distributed.DistributedMapReduceContextBuilder;
import com.continuuity.internal.app.runtime.batch.inmemory.InMemoryMapReduceContextBuilder;
import com.google.common.base.Throwables;
import com.google.gson.Gson;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MRConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;

/**
 *
 */
public class MapReduceContextProvider {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractMapReduceContextBuilder.class);

  private static final String HCONF_ATTR_RUN_ID = "hconf.program.run.id";
  private static final String HCONF_ATTR_CCONF = "hconf.cconf";
  private static final String HCONF_ATTR_INPUT_DATASET = "hconf.program.input.dataset";
  private static final String HCONF_ATTR_INPUT_SPLIT_CLASS = "hconf.program.input.split.class";
  private static final String HCONF_ATTR_INPUT_SPLITS = "hconf.program.input.splits";
  private static final String HCONF_ATTR_OUTPUT_DATASET = "hconf.program.output.dataset";

  private final JobContext jobContext;
  private AbstractMapReduceContextBuilder contextBuilder;

  private BasicMapReduceContext context;

  public MapReduceContextProvider(JobContext context) {
    this.jobContext = context;
  }

  public synchronized BasicMapReduceContext get() {
    if (context == null) {
      CConfiguration conf = getConf();
      context = getBuilder(conf)
        .build(conf,
               getRunId(),
               getProgramLocation(),
               getInputDataSet(),
               getInputSelection(),
               getOutputDataSet());
    }
    return context;
  }

  private String getProgramLocation() {
    return jobContext.getJar();
  }

  public void set(BasicMapReduceContext context, CConfiguration conf) {
    setRunId(context.getRunId().getId());
    setConf(conf);
    setInputDataSet(((DataSet)context.getInputDataset()).getName());
    setInputSelection(context.getInputDataSelection());
    setOutputDataSet(((DataSet)context.getInputDataset()).getName());
  }

  private void setRunId(String runId) {
    jobContext.getConfiguration().set(HCONF_ATTR_RUN_ID, runId);
  }

  private String getRunId() {
    return jobContext.getConfiguration().get(HCONF_ATTR_RUN_ID);
  }

  private void setInputDataSet(String dataSetName) {
    jobContext.getConfiguration().set(HCONF_ATTR_INPUT_DATASET, dataSetName);
  }

  private String getInputDataSet() {
    return jobContext.getConfiguration().get(HCONF_ATTR_INPUT_DATASET);
  }

  private void setInputSelection(List<Split> splits) {
    // todo: this is ugly
    Class<? extends Split> splitClass;
    if (splits.size() > 0) {
      splitClass = splits.get(0).getClass();
    } else {
      // assign any
      splitClass = SimpleSplit.class;
    }
    jobContext.getConfiguration().set(HCONF_ATTR_INPUT_SPLIT_CLASS, splitClass.getName());

    // todo: re-use Gson instance?
    jobContext.getConfiguration().set(HCONF_ATTR_INPUT_SPLITS, new Gson().toJson(splits));
  }

  private List<Split> getInputSelection() {
    String splitClassName = jobContext.getConfiguration().get(HCONF_ATTR_INPUT_SPLIT_CLASS);
    String splitsJson = jobContext.getConfiguration().get(HCONF_ATTR_INPUT_SPLITS);

    try {
      // Yes, we know that it implements Split
      @SuppressWarnings("unchecked")
      Class<? extends Split> splitClass = (Class<? extends Split>) Class.forName(splitClassName);
      return new Gson().fromJson(splitsJson, new ListSplitType(splitClass));
    } catch (ClassNotFoundException e) {
      //todo
      throw Throwables.propagate(e);
    }
  }

  // This is needed to deserialize JSON into generified List
  private static final class ListSplitType implements ParameterizedType {
    private final Class<? extends Split> implementationClass;

    private ListSplitType(Class<? extends Split> implementationClass) {
      this.implementationClass = implementationClass;
    }

    @Override
    public Type[] getActualTypeArguments() {
      return new Type[]{implementationClass};
    }

    @Override
    public Type getRawType() {
      return List.class;
    }

    @Override
    public Type getOwnerType() {
      // it is fine, as it is not inner class
      return null;
    }
  }

  private void setOutputDataSet(String dataSetName) {
    jobContext.getConfiguration().set(HCONF_ATTR_OUTPUT_DATASET, dataSetName);
  }

  private String getOutputDataSet() {
    return jobContext.getConfiguration().get(HCONF_ATTR_OUTPUT_DATASET);
  }

  private void setConf(CConfiguration conf) {
    StringWriter stringWriter = new StringWriter();
    try {
      conf.writeXml(stringWriter);
    } catch (IOException e) {
      LOG.error("Unable to serialize CConfiguration into xml");
      throw Throwables.propagate(e);
    }
    jobContext.getConfiguration().set(HCONF_ATTR_CCONF, stringWriter.toString());
  }

  private CConfiguration getConf() {
    CConfiguration conf = new CConfiguration();
    conf.addResource(new ByteArrayInputStream(jobContext.getConfiguration().get(HCONF_ATTR_CCONF).getBytes()));
    return conf;
  }

  private synchronized AbstractMapReduceContextBuilder getBuilder(CConfiguration conf) {
    if (contextBuilder == null) {
      String mrFramework = jobContext.getConfiguration().get(MRConfig.FRAMEWORK_NAME, MRConfig.LOCAL_FRAMEWORK_NAME);
      if ("local".equals(mrFramework)) {
        contextBuilder = new InMemoryMapReduceContextBuilder(conf);
      } else {
        // mrFramework = "yarn" or "classic"
        contextBuilder = new DistributedMapReduceContextBuilder(conf, jobContext.getConfiguration());
      }
    }
    return contextBuilder;
  }

}
