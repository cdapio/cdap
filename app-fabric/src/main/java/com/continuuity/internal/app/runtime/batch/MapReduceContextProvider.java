package com.continuuity.internal.app.runtime.batch;

import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.batch.SimpleSplit;
import com.continuuity.api.data.batch.Split;
import com.continuuity.app.runtime.Arguments;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.operation.executor.ReadPointer;
import com.continuuity.data.operation.executor.Transaction;
import com.continuuity.data.operation.executor.omid.memory.MemoryReadPointer;
import com.continuuity.internal.app.runtime.BasicArguments;
import com.continuuity.internal.app.runtime.batch.distributed.DistributedMapReduceContextBuilder;
import com.continuuity.internal.app.runtime.batch.inmemory.InMemoryMapReduceContextBuilder;
import com.google.common.base.Throwables;
import com.google.gson.Gson;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MRConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Provides access to MapReduceContext for mapreduce job tasks.
 */
public final class MapReduceContextProvider {

  private static final Logger LOG = LoggerFactory.getLogger(MapReduceContextProvider.class);

  private static final String HCONF_ATTR_RUN_ID = "hconf.program.run.id";
  private static final String HCONF_ATTR_ARGS = "hconf.program.args";
  private static final String HCONF_ATTR_PROGRAM_JAR_NAME = "hconf.program.jar.name";
  private static final String HCONF_ATTR_CCONF = "hconf.cconf";
  private static final String HCONF_ATTR_INPUT_DATASET = "hconf.program.input.dataset";
  private static final String HCONF_ATTR_INPUT_SPLIT_CLASS = "hconf.program.input.split.class";
  private static final String HCONF_ATTR_INPUT_SPLITS = "hconf.program.input.splits";
  private static final String HCONF_ATTR_OUTPUT_DATASET = "hconf.program.output.dataset";

  private static final String HCONF_ATTR_TX_WRITE_VERSION = "hconf.program.tx.write_version";
  private static final String HCONF_ATTR_TX_READ_POINTER_WRITE_POINT = "hconf.program.tx.read_pointer.write";
  private static final String HCONF_ATTR_TX_READ_POINTER_READ_POINT = "hconf.program.tx.read_pointer.read";
  private static final String HCONF_ATTR_TX_READ_POINTER_EXCLUDES = "hconf.program.tx.read_pointer.excludes";

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
               getAruments(),
               getTx(),
               jobContext.getConfiguration().getClassLoader(),
               getProgramLocation(),
               getInputDataSet(),
               getInputSelection(),
               getOutputDataSet());
    }
    return context;
  }

  public void set(BasicMapReduceContext context, CConfiguration conf, Transaction tx, String programJarName) {
    setRunId(context.getRunId().getId());
    setArguments(context.getRuntimeArgs());
    setProgramJarName(programJarName);
    setConf(conf);
    setTx(tx);
    if (context.getInputDataset() != null) {
      setInputDataSet(((DataSet) context.getInputDataset()).getName());
      if (context.getInputDataSelection() != null) {
        setInputSelection(context.getInputDataSelection());
      }
    }
    if (context.getOutputDataset() != null) {
      setOutputDataSet(((DataSet) context.getOutputDataset()).getName());
    }
  }

  private void setArguments(Arguments runtimeArgs) {
    jobContext.getConfiguration().set(HCONF_ATTR_ARGS, new Gson().toJson(runtimeArgs));
  }

  private Arguments getAruments() {
    return new Gson().fromJson(jobContext.getConfiguration().get(HCONF_ATTR_ARGS), BasicArguments.class);
  }

  private String getProgramLocation() {
    String programJarName = getProgramJarName();
    for (Path file : jobContext.getFileClassPaths()) {
      if (programJarName.equals(file.getName())) {
        return file.toUri().getPath();
      }
    }
    throw new IllegalStateException("Program jar " + programJarName + " not found in classpath files.");
  }

  private void setRunId(String runId) {
    jobContext.getConfiguration().set(HCONF_ATTR_RUN_ID, runId);
  }

  private String getRunId() {
    return jobContext.getConfiguration().get(HCONF_ATTR_RUN_ID);
  }

  private void setProgramJarName(String programJarName) {
    jobContext.getConfiguration().set(HCONF_ATTR_PROGRAM_JAR_NAME, programJarName);
  }

  private String getProgramJarName() {
    return jobContext.getConfiguration().get(HCONF_ATTR_PROGRAM_JAR_NAME);
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
    if (splitClassName == null || splitsJson == null) {
      return Collections.emptyList();
    }

    try {
      // Yes, we know that it implements Split
      @SuppressWarnings("unchecked")
      Class<? extends Split> splitClass =
        (Class<? extends Split>) jobContext.getConfiguration().getClassLoader().loadClass(splitClassName);
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
    CConfiguration conf = CConfiguration.create();
    conf.addResource(new ByteArrayInputStream(jobContext.getConfiguration().get(HCONF_ATTR_CCONF).getBytes()));
    return conf;
  }

  private void setTx(Transaction tx) {
    ReadPointer readPointer = tx.getReadPointer();
    if (!(readPointer instanceof MemoryReadPointer)) {
      String message = String.format("Unsupported readPointer implementation %s, only MemoryReadPointer is supported",
                                     readPointer.getClass().getName());
      LOG.error(message);
      throw new IllegalArgumentException(message);

    }
    jobContext.getConfiguration().setLong(HCONF_ATTR_TX_WRITE_VERSION, tx.getWriteVersion());
    jobContext.getConfiguration().setLong(HCONF_ATTR_TX_READ_POINTER_WRITE_POINT,
                                          ((MemoryReadPointer) readPointer).getWriteVersion());
    jobContext.getConfiguration().setLong(HCONF_ATTR_TX_READ_POINTER_READ_POINT,
                                          ((MemoryReadPointer) readPointer).getReadPointer());
    jobContext.getConfiguration().set(HCONF_ATTR_TX_READ_POINTER_EXCLUDES,
                                      new Gson().toJson(((MemoryReadPointer) readPointer).getReadExcludes()));
  }

  private Transaction getTx() {
    long writeVersion = Long.valueOf(jobContext.getConfiguration().get(HCONF_ATTR_TX_WRITE_VERSION));
    long writePoint = Long.valueOf(jobContext.getConfiguration().get(HCONF_ATTR_TX_READ_POINTER_WRITE_POINT));
    long readPoint = Long.valueOf(jobContext.getConfiguration().get(HCONF_ATTR_TX_READ_POINTER_READ_POINT));
    @SuppressWarnings("unchecked")
    Set<Long> excludes = new Gson().fromJson(jobContext.getConfiguration().get(HCONF_ATTR_TX_READ_POINTER_EXCLUDES),
                                             HashSet.class);
    // we want long-running transaction which doesn't track changes
    return new Transaction(writeVersion, new MemoryReadPointer(readPoint, writePoint, excludes), false);
  }

  private synchronized AbstractMapReduceContextBuilder getBuilder(CConfiguration conf) {
    if (contextBuilder == null) {
      String mrFramework = jobContext.getConfiguration().get(MRConfig.FRAMEWORK_NAME, MRConfig.LOCAL_FRAMEWORK_NAME);
      if ("local".equals(mrFramework)) {
        contextBuilder = new InMemoryMapReduceContextBuilder(conf);
      } else {
        // mrFramework = "yarn" or "classic"
        contextBuilder =
          new DistributedMapReduceContextBuilder(conf, HBaseConfiguration.create(jobContext.getConfiguration()));
      }
    }
    return contextBuilder;
  }

}
