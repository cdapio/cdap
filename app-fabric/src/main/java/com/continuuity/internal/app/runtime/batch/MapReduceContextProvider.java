package com.continuuity.internal.app.runtime.batch;

import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.batch.SimpleSplit;
import com.continuuity.api.data.batch.Split;
import com.continuuity.app.runtime.Arguments;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.operation.executor.ReadPointer;
import com.continuuity.data.operation.executor.omid.memory.MemoryReadPointer;
import com.continuuity.data2.transaction.Transaction;
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
import java.net.URI;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Provides access to MapReduceContext for mapreduce job tasks.
 */
public final class MapReduceContextProvider {

  private static final Logger LOG = LoggerFactory.getLogger(MapReduceContextProvider.class);

  private static final class ConfigKeys {

    static final String RUN_ID = "hconf.program.run.id";
    static final String ARGS = "hconf.program.args";
    static final String PROGRAM_JAR_NAME = "hconf.program.jar.name";
    static final String CCONF = "hconf.cconf";
    static final String INPUT_DATASET = "hconf.program.input.dataset";
    static final String INPUT_SPLIT_CLASS = "hconf.program.input.split.class";
    static final String INPUT_SPLITS = "hconf.program.input.splits";
    static final String OUTPUT_DATASET = "hconf.program.output.dataset";
    static final String LOGICAL_START_TIME = "hconf.program.logical.start.time";
    static final String TX_WRITE_VERSION = "hconf.program.tx.write_version";
    static final String TX_READ_POINTER_WRITE_POINT = "hconf.program.tx.read_pointer.write";
    static final String TX_READ_POINTER_READ_POINT = "hconf.program.tx.read_pointer.read";
    static final String TX_READ_POINTER_EXCLUDES = "hconf.program.tx.read_pointer.excludes";
    static final String NEW_TX = "hconf.program.newtx.tx";
  }

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
               getLogicalStartTime(),
               getAruments(),
               getTx(),
               getTx2(),
               jobContext.getConfiguration().getClassLoader(),
               getProgramLocation(),
               getInputDataSet(),
               getInputSelection(),
               getOutputDataSet());
    }
    return context;
  }

  public void set(BasicMapReduceContext context, CConfiguration conf,
                  com.continuuity.data.operation.executor.Transaction tx,
                  Transaction tx2, String programJarName) {
    setRunId(context.getRunId().getId());
    setLogicalStartTime(context.getLogicalStartTime());
    setArguments(context.getRuntimeArgs());
    setProgramJarName(programJarName);
    setConf(conf);
    setTx(tx);
    setTx2(tx2);
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
    jobContext.getConfiguration().set(ConfigKeys.ARGS, new Gson().toJson(runtimeArgs));
  }

  private Arguments getAruments() {
    return new Gson().fromJson(jobContext.getConfiguration().get(ConfigKeys.ARGS), BasicArguments.class);
  }

  private URI getProgramLocation() {
    String programJarName = getProgramJarName();
    for (Path file : jobContext.getFileClassPaths()) {
      if (programJarName.equals(file.getName())) {
        return file.toUri();
      }
    }
    throw new IllegalStateException("Program jar " + programJarName + " not found in classpath files.");
  }

  private void setRunId(String runId) {
    jobContext.getConfiguration().set(ConfigKeys.RUN_ID, runId);
  }

  private String getRunId() {
    return jobContext.getConfiguration().get(ConfigKeys.RUN_ID);
  }

  private void setLogicalStartTime(long startTime) {
    jobContext.getConfiguration().setLong(ConfigKeys.LOGICAL_START_TIME, startTime);
  }

  private long getLogicalStartTime() {
    return jobContext.getConfiguration().getLong(ConfigKeys.LOGICAL_START_TIME, System.currentTimeMillis());
  }

  private void setProgramJarName(String programJarName) {
    jobContext.getConfiguration().set(ConfigKeys.PROGRAM_JAR_NAME, programJarName);
  }

  private String getProgramJarName() {
    return jobContext.getConfiguration().get(ConfigKeys.PROGRAM_JAR_NAME);
  }

  private void setInputDataSet(String dataSetName) {
    jobContext.getConfiguration().set(ConfigKeys.INPUT_DATASET, dataSetName);
  }

  private String getInputDataSet() {
    return jobContext.getConfiguration().get(ConfigKeys.INPUT_DATASET);
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
    jobContext.getConfiguration().set(ConfigKeys.INPUT_SPLIT_CLASS, splitClass.getName());

    // todo: re-use Gson instance?
    jobContext.getConfiguration().set(ConfigKeys.INPUT_SPLITS, new Gson().toJson(splits));
  }

  private List<Split> getInputSelection() {
    String splitClassName = jobContext.getConfiguration().get(ConfigKeys.INPUT_SPLIT_CLASS);
    String splitsJson = jobContext.getConfiguration().get(ConfigKeys.INPUT_SPLITS);
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
    jobContext.getConfiguration().set(ConfigKeys.OUTPUT_DATASET, dataSetName);
  }

  private String getOutputDataSet() {
    return jobContext.getConfiguration().get(ConfigKeys.OUTPUT_DATASET);
  }

  private void setConf(CConfiguration conf) {
    StringWriter stringWriter = new StringWriter();
    try {
      conf.writeXml(stringWriter);
    } catch (IOException e) {
      LOG.error("Unable to serialize CConfiguration into xml");
      throw Throwables.propagate(e);
    }
    jobContext.getConfiguration().set(ConfigKeys.CCONF, stringWriter.toString());
  }

  private CConfiguration getConf() {
    CConfiguration conf = CConfiguration.create();
    conf.addResource(new ByteArrayInputStream(jobContext.getConfiguration().get(ConfigKeys.CCONF).getBytes()));
    return conf;
  }

  private void setTx(com.continuuity.data.operation.executor.Transaction tx) {
    ReadPointer readPointer = tx.getReadPointer();
    if (!(readPointer instanceof MemoryReadPointer)) {
      String message = String.format("Unsupported readPointer implementation %s, only MemoryReadPointer is supported",
                                     readPointer.getClass().getName());
      LOG.error(message);
      throw new IllegalArgumentException(message);

    }
    jobContext.getConfiguration().setLong(ConfigKeys.TX_WRITE_VERSION, tx.getWriteVersion());
    jobContext.getConfiguration().setLong(ConfigKeys.TX_READ_POINTER_WRITE_POINT,
                                          ((MemoryReadPointer) readPointer).getWriteVersion());
    jobContext.getConfiguration().setLong(ConfigKeys.TX_READ_POINTER_READ_POINT,
                                          ((MemoryReadPointer) readPointer).getReadPointer());
    jobContext.getConfiguration().set(ConfigKeys.TX_READ_POINTER_EXCLUDES,
                                      new Gson().toJson(((MemoryReadPointer) readPointer).getReadExcludes()));
  }

  private com.continuuity.data.operation.executor.Transaction getTx() {
    long writeVersion = Long.valueOf(jobContext.getConfiguration().get(ConfigKeys.TX_WRITE_VERSION));
    long writePoint = Long.valueOf(jobContext.getConfiguration().get(ConfigKeys.TX_READ_POINTER_WRITE_POINT));
    long readPoint = Long.valueOf(jobContext.getConfiguration().get(ConfigKeys.TX_READ_POINTER_READ_POINT));
    @SuppressWarnings("unchecked")
    Set<Long> excludes = new Gson().fromJson(jobContext.getConfiguration().get(ConfigKeys.TX_READ_POINTER_EXCLUDES),
                                             HashSet.class);
    // we want long-running transaction which doesn't track changes
    return new com.continuuity.data.operation.executor.Transaction(
      writeVersion,
      new MemoryReadPointer(readPoint, writePoint, excludes), false);
  }

  private void setTx2(Transaction tx) {
    jobContext.getConfiguration().set(ConfigKeys.NEW_TX, tx.toJson());
  }

  private Transaction getTx2() {
    return Transaction.fromJson(jobContext.getConfiguration().get(ConfigKeys.NEW_TX));
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
