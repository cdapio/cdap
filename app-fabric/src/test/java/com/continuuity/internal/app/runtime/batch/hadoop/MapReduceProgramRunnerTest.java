package com.continuuity.internal.app.runtime.batch.hadoop;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.dataset.KeyValueTable;
import com.continuuity.api.data.dataset.SimpleTimeseriesTable;
import com.continuuity.api.data.dataset.TimeseriesTable;
import com.continuuity.app.program.Program;
import com.continuuity.app.runtime.ProgramController;
import com.continuuity.app.runtime.ProgramRunner;
import com.continuuity.data.DataFabricImpl;
import com.continuuity.data.dataset.DataSetInstantiator;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.executor.SynchronousTransactionAgent;
import com.continuuity.data.operation.executor.TransactionProxy;
import com.continuuity.internal.app.deploy.pipeline.ApplicationWithPrograms;
import com.continuuity.internal.app.runtime.BasicArguments;
import com.continuuity.internal.app.runtime.ProgramRunnerFactory;
import com.continuuity.internal.app.runtime.SimpleProgramOptions;
import com.continuuity.test.internal.DefaultId;
import com.continuuity.test.internal.TestHelper;
import com.continuuity.weave.filesystem.LocationFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.inject.Injector;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class MapReduceProgramRunnerTest {
  private static Injector injector = TestHelper.getInjector();

  @Test
  public void testWordCount() throws Exception {
    final ApplicationWithPrograms app = TestHelper.deployApplicationWithManager(AppWithMapReduce.class);

    OperationExecutor opex = injector.getInstance(OperationExecutor.class);
    LocationFactory locationFactory = injector.getInstance(LocationFactory.class);
    OperationContext opCtx = new OperationContext(DefaultId.ACCOUNT.getId(),
                                                  app.getAppSpecLoc().getSpecification().getName());

    String inputPath = createInput();
    File outputDir = new File(FileUtils.getTempDirectory().getPath() + "/out_" + System.currentTimeMillis());
    outputDir.deleteOnExit();

    KeyValueTable jobConfigTable = (KeyValueTable) getTable(opex, opCtx, locationFactory, "jobConfig");
    jobConfigTable.write(tb("inputPath"), tb(inputPath));
    jobConfigTable.write(tb("outputPath"), tb(outputDir.getPath()));

    runProgram(app, AppWithMapReduce.ClassicWordCount.class);

    File outputFile = outputDir.listFiles()[0];
    int lines = 0;
    BufferedReader reader = new BufferedReader(new FileReader(outputFile));
    try {
      while (true) {
        String line = reader.readLine();
        if (line == null) {
          break;
        }
        lines++;
      }
    } finally {
      reader.close();
    }
    // dummy check that output file is not empty
    Assert.assertTrue(lines > 0);
  }

  @Test
  public void testTimeSeriesRecordsCount() throws Exception {
    final ApplicationWithPrograms app = TestHelper.deployApplicationWithManager(AppWithMapReduce.class);

    OperationExecutor opex = injector.getInstance(OperationExecutor.class);
    LocationFactory locationFactory = injector.getInstance(LocationFactory.class);
    OperationContext opCtx = new OperationContext(DefaultId.ACCOUNT.getId(),
                                                  app.getAppSpecLoc().getSpecification().getName());

    TimeseriesTable table = (TimeseriesTable) getTable(opex, opCtx, locationFactory, "timeSeries");

    fillTestInputData(table);

    Thread.sleep(2);

    long start = System.currentTimeMillis();
    runProgram(app, AppWithMapReduce.AggregateTimeseriesByTag.class);
    long stop = System.currentTimeMillis();

    Map<String, Long> expected = Maps.newHashMap();
    // note: not all records add to the sum since filter by tag="tag1" and ts={1..3} is used
    expected.put("tag1", 18L);
    expected.put("tag2", 3L);
    expected.put("tag3", 18L);

    List<TimeseriesTable.Entry> agg = table.read(AggregateMetricsByTag.BY_TAGS, start, stop);
    Assert.assertEquals(expected.size(), agg.size());
    for (TimeseriesTable.Entry entry : agg) {
      String tag = Bytes.toString(entry.getTags()[0]);
      Assert.assertEquals((long) expected.get(tag), Bytes.toLong(entry.getValue()));
    }
  }

  private void fillTestInputData(TimeseriesTable table) throws OperationException {
    byte[] metric1 = Bytes.toBytes("metric");
    byte[] metric2 = Bytes.toBytes("metric2");
    byte[] tag1 = Bytes.toBytes("tag1");
    byte[] tag2 = Bytes.toBytes("tag2");
    byte[] tag3 = Bytes.toBytes("tag3");
    // m1e1 = metric: 1, entity: 1
    SimpleTimeseriesTable.Entry m1e1 =
      new SimpleTimeseriesTable.Entry(metric1, Bytes.toBytes(3L), 1, tag3, tag2, tag1);
    table.write(m1e1);
    SimpleTimeseriesTable.Entry m1e2 =
      new SimpleTimeseriesTable.Entry(metric1, Bytes.toBytes(10L), 2, tag2, tag3);
    table.write(m1e2);
    SimpleTimeseriesTable.Entry m1e3 =
      new SimpleTimeseriesTable.Entry(metric1, Bytes.toBytes(15L), 3, tag1, tag3);
    table.write(m1e3);
    SimpleTimeseriesTable.Entry m1e4 =
      new SimpleTimeseriesTable.Entry(metric1, Bytes.toBytes(23L), 4, tag2);
    table.write(m1e4);

    SimpleTimeseriesTable.Entry m2e1 =
      new SimpleTimeseriesTable.Entry(metric2, Bytes.toBytes(4L), 3, tag1, tag3);
    table.write(m2e1);
  }

  private void runProgram(ApplicationWithPrograms app, Class<?> programClass) throws Exception {
    waitForCompletion(submit(app, programClass));
  }

  private void waitForCompletion(ProgramController controller) throws InterruptedException {
    while (controller.getState() == ProgramController.State.ALIVE) {
      TimeUnit.SECONDS.sleep(1);
    }
  }

  private ProgramController submit(ApplicationWithPrograms app, Class<?> programClass) throws ClassNotFoundException {
    ProgramRunnerFactory runnerFactory = injector.getInstance(ProgramRunnerFactory.class);
    final Program program = getProgram(app, programClass);
    ProgramRunner runner = runnerFactory.create(ProgramRunnerFactory.Type.valueOf(program.getProcessorType().name()));

    HashMap<String, String> userArgs = Maps.newHashMap();
    userArgs.put("metric", "metric");
    userArgs.put("startTs", "1");
    userArgs.put("stopTs", "3");
    userArgs.put("tag", "tag1");
    return runner.run(program, new SimpleProgramOptions(program.getProgramName(),
                                                        new BasicArguments(),
                                                        new BasicArguments(userArgs)));
  }

  private Program getProgram(ApplicationWithPrograms app, Class<?> programClass) throws ClassNotFoundException {
    for (Program p : app.getPrograms()) {
      if (programClass.getCanonicalName().equals(p.getMainClass().getCanonicalName())) {
        return p;
      }
    }
    return null;
  }

  private byte[] tb(String val) {
    return Bytes.toBytes(val);
  }

  private String createInput() throws IOException {
    File inputDir = new File(FileUtils.getTempDirectory().getPath() + "/in_" + System.currentTimeMillis());
    inputDir.mkdirs();
    inputDir.deleteOnExit();

    File inputFile = new File(inputDir.getPath() + "/words.txt");
    inputFile.deleteOnExit();
    BufferedWriter writer = new BufferedWriter(new FileWriter(inputFile));
    try {
      writer.write("this text has");
      writer.newLine();
      writer.write("two words text inside");
    } finally {
      writer.close();
    }

    return inputDir.getPath();
  }

  private DataSet getTable(OperationExecutor opex, OperationContext opCtx,
                           LocationFactory locationFactory, String tableName) {
    TransactionProxy proxy = new TransactionProxy();
    proxy.setTransactionAgent(new SynchronousTransactionAgent(opex, opCtx));
    DataSetInstantiator dataSetInstantiator = new DataSetInstantiator(new DataFabricImpl(opex, locationFactory, opCtx),
                                                                      proxy,
                                                                      getClass().getClassLoader());
    dataSetInstantiator.setDataSets(ImmutableList.copyOf(new AppWithMapReduce().configure().getDataSets().values()));

    return dataSetInstantiator.getDataSet(tableName);
  }
}
