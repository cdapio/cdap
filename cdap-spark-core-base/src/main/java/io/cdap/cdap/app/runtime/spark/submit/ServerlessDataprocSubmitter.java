package io.cdap.cdap.app.runtime.spark.submit;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.common.util.concurrent.Uninterruptibles;
import io.cdap.cdap.api.spark.SparkSpecification;
import io.cdap.cdap.app.runtime.Arguments;
import io.cdap.cdap.app.runtime.spark.SparkMainWrapper;
import io.cdap.cdap.app.runtime.spark.SparkRuntimeContext;
import io.cdap.cdap.app.runtime.spark.SparkRuntimeContextConfig;
import io.cdap.cdap.app.runtime.spark.distributed.SparkExecutionService;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.lang.ClassLoaders;
import io.cdap.cdap.internal.app.runtime.distributed.LocalizeResource;
import io.cdap.cdap.internal.app.runtime.workflow.BasicWorkflowToken;
import io.cdap.cdap.internal.app.runtime.workflow.WorkflowProgramInfo;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.runtime.spi.runtimejob.LaunchMode;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.deploy.SparkSubmit;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

public class ServerlessDataprocSubmitter extends AbstractSparkSubmitter {

  private static final Logger LOG = LoggerFactory.getLogger(ServerlessDataprocSubmitter.class);

  private final Configuration hConf;
  private final String schedulerQueueName;
  private final SparkExecutionService sparkExecutionService;
  private final long tokenRenewalInterval;
  private final LaunchMode launchMode;

  // Transforms LocalizeResource to URI string
  private static final Function<LocalizeResource, String> RESOURCE_TO_PATH =
    input -> input.getURI().toString().split("#")[0];

  public ServerlessDataprocSubmitter(Configuration hConf, LocationFactory locationFactory,
                                     String hostname, SparkRuntimeContext runtimeContext,
                                     @Nullable String schedulerQueueName,
                                     LaunchMode launchMode) {
    this.hConf = hConf;
    this.schedulerQueueName = schedulerQueueName;
    ProgramRunId programRunId = runtimeContext.getProgram().getId().run(runtimeContext.getRunId().getId());
    WorkflowProgramInfo workflowInfo = runtimeContext.getWorkflowInfo();
    BasicWorkflowToken workflowToken = workflowInfo == null ? null : workflowInfo.getWorkflowToken();
    this.sparkExecutionService = new SparkExecutionService(locationFactory, hostname, programRunId, workflowToken);

    Arguments systemArgs = runtimeContext.getProgramOptions().getArguments();
    this.tokenRenewalInterval = systemArgs.hasOption(SparkRuntimeContextConfig.CREDENTIALS_UPDATE_INTERVAL_MS)
      ? Long.parseLong(systemArgs.getOption(SparkRuntimeContextConfig.CREDENTIALS_UPDATE_INTERVAL_MS))
      : -1L;
    this.launchMode = launchMode;
  }

  @Override
  protected void addMaster(Map<String, String> configs, ImmutableList.Builder<String> argBuilder) throws Exception {
  }

  @Override
  protected void triggerShutdown(long timeout, TimeUnit timeoutTimeUnit) {

  }



  //
  @Override
  public final <V> SparkJobFuture<V> submit(SparkRuntimeContext runtimeContext,
                                            Map<String, String> configs, List<LocalizeResource> resources,
                                            URI jobFile, final V result) throws Exception {
    SparkSpecification spec = runtimeContext.getSparkSpecification();

    SparkConf sparkConf = createSparkConf(runtimeContext, configs, resources, jobFile);

    // Spark submit is called from this executor
    // Use an executor to simplify logic that is needed to interrupt the running thread on stopping
    ExecutorService executor = Executors.newSingleThreadExecutor(
      new ThreadFactoryBuilder()
        .setNameFormat("spark-submitter-" + spec.getName() + "-" + runtimeContext.getRunId())
        .build());

    // Latch for the Spark job completion
    CountDownLatch completion = new CountDownLatch(1);
    long defaultTimeoutMillis = TimeUnit.SECONDS.toMillis(
      runtimeContext.getCConfiguration().getLong(Constants.AppFabric.PROGRAM_MAX_STOP_SECONDS));

    AbstractSparkJobFuture<V> resultFuture = new AbstractSparkJobFuture<V>(defaultTimeoutMillis) {
      @Override
      protected void onCancel(long timeout, TimeUnit timeoutTimeUnit) {
        runtimeContext.setTerminationTime(System.currentTimeMillis() + timeoutTimeUnit.toMillis(timeout));

        // Try to shutdown the running spark job.
        triggerShutdown(timeout, timeoutTimeUnit);

        // Wait for the Spark-Submit returns
        Uninterruptibles.awaitUninterruptibly(completion);
      }
    };

    // Submit the Spark job
    executor.submit(() -> {
      try {
        List<String> extraArgs = beforeSubmit(); // TODO : handle to include this

//        String[] submitArgs = Iterables.toArray(Iterables.concat(args, extraArgs), String.class);
        submit(runtimeContext, sparkConf);
        boolean state = waitForFinish();
        if (!state) {
          throw new Exception("Spark driver returned error state");
        }
        onCompleted(state);
        resultFuture.complete(result);
      } catch (Throwable t) {
        onCompleted(false);
        resultFuture.completeExceptionally(t);
      } finally {
        completion.countDown();
      }
    });
    // Shutdown the executor right after submit since the thread is only used for one submission.
    executor.shutdown();
    return resultFuture;
  }


  /**
   * Submits the Spark job using {@link SparkSubmit}.
   *
   * @param runtimeContext context representing the Spark program
//   * @param args arguments for the {@link SparkSubmit#main(String[])} method.
   */
  private void submit(SparkRuntimeContext runtimeContext, SparkConf sparkConf) {
    ClassLoader oldClassLoader = ClassLoaders.setContextClassLoader(runtimeContext.getProgramInvocationClassLoader());
    try {
      // TODO  HACk :
      // Caused by: java.lang.IllegalArgumentException: requirement failed:
      // com.google.cloud.spark.performance.DataprocMetricsListener is not a subclass of
      // org.apache.spark.scheduler.SparkListenerInterface.
      sparkConf.set("spark.dataproc.listeners","")
        .set("spark.shuffle.service.enabled", "false")
        // TODO : Initial job has not accepted any resources; check your cluster UI to ensure that workers are registered and have sufficient resources
        .set("spark.dynamicAllocation.enabled", "false")
      // TODO : error :  '-Xlog:gc*:file=<LOG_DIR>/gc.log:time,level,tags:filecount=10,filesize=1M', see error log for details.
      .set("spark.driver.extraJavaOptions", "-XX:+UseG1GC -verbose:gc -Xlog:gc*:file=/tmp/gc.log:time,level,tags:filecount=10,filesize=1M -XX:+ExitOnOutOfMemoryError -Dstreaming.checkpoint.rewrite.enabled=true")
      .set("spark.executor.extraJavaOptions","-XX:+UseG1GC -verbose:gc -Xlog:gc*:file=/tmp/gc.log:time,level,tags:filecount=10,filesize=1M -XX:+ExitOnOutOfMemoryError -Dstreaming.checkpoint.rewrite.enabled=true")
        // TODO :  error : 2023-08-11 10:48:57,467 - WARN  [task-result-getter-0:o.a.s.s.TaskSetManager@72] : java.lang.IllegalStateException: unread block data
        //https://groups.google.com/g/spark-users/c/jA1_UoIscuQ https://spark.apache.org/docs/latest/tuning.html
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
      LOG.warn("Calling SparkSubmit for {} {}: {}",
               runtimeContext.getProgram().getId(), runtimeContext.getRunId(), sparkConf.getAll());

      System.setProperty("SPARK_SUBMIT", "true");
//      SparkSubmit.main(args);

      System.setProperty("SPARK_SUBMIT", "true");
//      System.setProperty("spark.serializer", "spark.KryoSerializer");


      SparkSession sparkSession = SparkSession
        .builder()
        .config(sparkConf)
        .getOrCreate();

          List<String> data = Arrays.asList("abc", "abc", "xyz");
          Dataset<String> dataDs = sparkSession.createDataset(data, Encoders.STRING());
          dataDs.show();

      SparkMainWrapper.directSparkInitialize(sparkSession, runtimeContext.getSparkSpecification().getMainClassName());

      LOG.warn("SparkSubmit returned for {} {}", runtimeContext.getProgram().getId(), runtimeContext.getRunId());
    } finally {
      ClassLoaders.setContextClassLoader(oldClassLoader);
    }
  }







  //https://spark.apache.org/docs/latest/configuration.html
  //https://stackoverflow.com/questions/37132559/add-jar-files-to-a-spark-job-spark-submit
  protected SparkConf createSparkConf(SparkRuntimeContext runtimeContext, Map<String, String> configs,
                                               List<LocalizeResource> resources, URI jobFile) throws Exception {
    SparkSpecification spec = runtimeContext.getSparkSpecification();
    SparkConf sparkConf = new SparkConf();
//    ImmutableList.Builder<String> builder = ImmutableList.builder();
    Iterable<LocalizeResource> archivesIterable = getArchives(resources);
    Iterable<LocalizeResource> filesIterable = getFiles(resources);

//    addMasterPOC(configs, builder);

    sparkConf.set("spark.app.name", spec.getName());
//    builder.add("--conf").add("spark.app.name=" + spec.getName());

    configs.putAll(generateSubmitConf());
    BiConsumer<String, String> confAdder = (k, v) -> sparkConf.set(k , v);
    configs.forEach(confAdder);

    String archives = Joiner.on(',').join(Iterables.transform(archivesIterable, RESOURCE_TO_PATH));
    LOG.warn(" SANKET : archives : {}", archives);
    String files = Joiner.on(',').join(Iterables.transform(filesIterable, RESOURCE_TO_PATH));

    LOG.warn(" SANKET : files : {}", files);

    if (!Strings.isNullOrEmpty(archives)) {
      sparkConf.set("spark.archives", archives);
//      builder.add("--archives").add(archives);
    }
    if (!Strings.isNullOrEmpty(files)) {
//      builder.add("--files").add(files);
      sparkConf.set("spark.files", files);
    }

    URI newJobFile = getJobFile();
    if (newJobFile != null) {
      jobFile = newJobFile;
    }
//
//    boolean isPySpark = jobFile.getPath().endsWith(".py");
//    if (isPySpark) {
//      // For python, add extra py library files
//      String pyFiles = configs.get("spark.submit.pyFiles");
//      if (pyFiles != null) {
//        builder.add("--py-files").add(pyFiles);
//      }
//    } else {
//      builder.add("--class").add(SparkMainWrapper.class.getName());
//    }
//
    if ("file".equals(jobFile.getScheme())) {
      sparkConf.setJars(new String[] {jobFile.getPath()});
//      builder.add(jobFile.getPath());
    } else {
      sparkConf.setJars(new String[] {jobFile.toString()});
//      builder.add(jobFile.toString());
    }
//
//    if (!isPySpark) {
//      // Add extra arguments for easily identifying the program from command line.
//      // Arguments to user program is always coming from the runtime arguments.
//      builder.add("--cdap.spark.program=" + runtimeContext.getProgramRunId().toString());
//      builder.add("--cdap.user.main.class=" + spec.getMainClassName());
//    }

    return sparkConf;
  }


}
