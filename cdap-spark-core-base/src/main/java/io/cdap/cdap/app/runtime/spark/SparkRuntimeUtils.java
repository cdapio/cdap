/*
 * Copyright © 2016-2021 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.cdap.app.runtime.spark;

import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.common.util.concurrent.Uninterruptibles;
import com.google.gson.Gson;
import io.cdap.cdap.api.spark.SparkExecutionContext;
import io.cdap.cdap.app.runtime.spark.classloader.SparkClassRewriter;
import io.cdap.cdap.app.runtime.spark.classloader.SparkRunnerClassLoader;
import io.cdap.cdap.app.runtime.spark.distributed.SparkDriverService;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.lang.ClassLoaders;
import io.cdap.cdap.common.lang.jar.BundleJarUtil;
import io.cdap.cdap.common.service.Retries;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.internal.app.runtime.monitor.RuntimeClient;
import io.cdap.cdap.proto.id.ProgramRunId;
import org.apache.spark.SparkConf;
import org.apache.spark.scheduler.EventLoggingListener;
import org.apache.spark.scheduler.SparkListenerApplicationStart;
import org.apache.spark.streaming.DStreamGraph;
import org.apache.twill.common.Threads;
import org.apache.twill.internal.ServiceListenerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.Tuple2;
import scala.collection.parallel.TaskSupport;
import scala.collection.parallel.ThreadPoolTaskSupport;
import scala.collection.parallel.mutable.ParArray;

import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import javax.annotation.Nullable;

/**
 * Util class for common functions needed for Spark implementation.
 */
public final class SparkRuntimeUtils {

  public static final String CDAP_SPARK_EXECUTION_SERVICE_URI = "CDAP_SPARK_EXECUTION_SERVICE_URI";
  public static final String PYSPARK_PORT_FILE_NAME = "cdap.py4j.gateway.port.txt";
  public static final String PYSPARK_SECRET_FILE_NAME = "cdap.py4j.gateway.secret.txt";

  // Configuration option used to supply rewriting behavior in Drivers and Executors.
  // This configuration option is used in the SparkRuntimeService
  public static final String STREAMING_CHECKPOINT_REWRITE_ENABLED = "streaming.checkpoint.rewrite.enabled";
  // Configuration option used to control rewriting behavior in the cdap-site.xml file.
  public static final String SPARK_STREAMING_CHECKPOINT_REWRITE_ENABLED =
    "spark." + STREAMING_CHECKPOINT_REWRITE_ENABLED;

  private static final String LOCALIZED_RESOURCES = "spark.cdap.localized.resources";
  private static final int CHUNK_SIZE = 1 << 15;  // 32K
  private static final Logger LOG = LoggerFactory.getLogger(SparkRuntimeUtils.class);
  private static final Gson GSON = new Gson();

  /**
   * Creates a zip file which contains a serialized {@link Properties} with a given zip entry name, together with
   * all files under the given directory. This is called from Client.createConfArchive() as a workaround for the
   * SPARK-13441 bug.
   *
   * @param sparkConf the {@link SparkConf} to save
   * @param propertiesEntryName name of the zip entry for the properties
   * @param confDirPath directory to scan for files to include in the zip file
   * @param zipFile output file
   * @return the zip file
   */
  public static File createConfArchive(SparkConf sparkConf, final String propertiesEntryName,
                                       String confDirPath, final File zipFile) {
    final Properties properties = new Properties();
    for (Tuple2<String, String> tuple : sparkConf.getAll()) {
      properties.put(tuple._1(), tuple._2());
    }

    try {
      File confDir = new File(confDirPath);
      try (ZipOutputStream zipOutput = new ZipOutputStream(new FileOutputStream(zipFile))) {
        zipOutput.putNextEntry(new ZipEntry(propertiesEntryName));
        properties.store(zipOutput, "Spark configuration.");
        zipOutput.closeEntry();

        BundleJarUtil.addToArchive(confDir, zipOutput);
      }
      LOG.debug("Spark config archive created at {} from {}", zipFile, confDir);
      return zipFile;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Sets the {@link TaskSupport} for the given Scala {@link ParArray} to {@link ThreadPoolTaskSupport}.
   * This method is mainly used by {@link SparkRunnerClassLoader} to set the {@link TaskSupport} for the
   * parallel array used inside the {@link DStreamGraph} class in spark to avoid thread leakage after the
   * Spark program execution finished.
   */
  @SuppressWarnings("unused")
  public static <T> ParArray<T> setTaskSupport(ParArray<T> parArray) {
    ThreadPoolExecutor executor = new ThreadPoolExecutor(0, Integer.MAX_VALUE, 1,
                                                         TimeUnit.SECONDS, new SynchronousQueue<Runnable>(),
                                                         new ThreadFactoryBuilder()
                                                           .setNameFormat("task-support-%d").build());
    executor.allowCoreThreadTimeOut(true);
    parArray.tasksupport_$eq(new ThreadPoolTaskSupport(executor));
    return parArray;
  }

  /**
   * Saves the names of localized resources to the given config.
   */
  public static void setLocalizedResources(Set<String> localizedResourcesNames,
                                           Map<String, String> configs) {
    configs.put(LOCALIZED_RESOURCES, GSON.toJson(localizedResourcesNames));
  }

  /**
   * Retrieves the names of localized resources in the given config and constructs a map from the resource name
   * to local files with the resource names as the file names in the given directory.
   */
  public static Map<String, File> getLocalizedResources(File dir, SparkConf sparkConf) {
    String resources = sparkConf.get(LOCALIZED_RESOURCES, null);
    if (resources == null) {
      return Collections.emptyMap();
    }
    Map<String, File> result = new HashMap<>();
    Set<String> resourceNames = GSON.fromJson(resources, new TypeToken<Set<String>>() { }.getType());
    for (String name : resourceNames) {
      result.put(name, new File(dir, name));
    }
    return result;
  }

  /**
   * Initialize a Spark main() method. This is the first method to be called from the main() method of any
   * spark program.
   *
   * @return a {@link SparkProgramCompletion} to be called when user spark program completed.
   * The {@link SparkProgramCompletion#completed()} must be called when the user program returned normally,
   * while the {@link SparkProgramCompletion#completedWithException(Throwable)} must be called when the user program
   * raised exception.
   */
  public static SparkProgramCompletion initSparkMain() {
    final Thread mainThread = Thread.currentThread();
    SparkClassLoader sparkClassLoader;
    try {
      sparkClassLoader = SparkClassLoader.findFromContext();
    } catch (IllegalStateException e) {
      sparkClassLoader = SparkClassLoader.create();
    }

    final ClassLoader oldClassLoader = ClassLoaders.setContextClassLoader(
      sparkClassLoader.getRuntimeContext().getProgramInvocationClassLoader());
    final SparkExecutionContext sec = sparkClassLoader.getSparkExecutionContext(true);
    final SparkRuntimeContext runtimeContext = sparkClassLoader.getRuntimeContext();
    final Service driverService = createSparkDriverService(runtimeContext);

    // Watch for stopping of the driver service.
    // It can happen when a user program finished such that the Cancellable.cancel() returned by this method is called,
    // or it can happen when it received a stop command (distributed mode) in the SparkDriverService via heartbeat.
    // In local mode, the LocalSparkSubmitter calls the Cancellable.cancel() returned by this method directly
    // (via SparkMainWraper).
    // We use a service listener so that it can handle all cases.
    final CountDownLatch secStopLatch = new CountDownLatch(1);
    driverService.addListener(new ServiceListenerAdapter() {

      @Override
      public void stopping(Service.State from) {
        // Interrupt the driver main thread to signal for stopping if the driverService.stop() is not coming from
        // the main thread.
        if (Thread.currentThread() != mainThread) {
          LOG.debug("Issuing interrupt to mainThread '{}' to indicate a stop request.", mainThread.getName());
          mainThread.interrupt();
        }
      }

      @Override
      public void terminated(Service.State from) {
        handleStopped();
      }

      @Override
      public void failed(Service.State from, Throwable failure) {
        handleStopped();
      }

      private void handleStopped() {
        try {
          // Close the SparkExecutionContext (it will stop all the SparkContext and release all resources)
          if (sec instanceof AutoCloseable) {
            try {
              ((AutoCloseable) sec).close();
            } catch (Exception e) {
              // Just log. It shouldn't throw, and if it does (due to bug), nothing much can be done
              LOG.warn("Exception raised when calling {}.close() for program run {}.",
                       sec.getClass().getName(), runtimeContext.getProgramRunId(), e);
            }
          }
        } finally {
          secStopLatch.countDown();
        }
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    driverService.startAndWait();
    return new SparkProgramCompletion() {
      @Override
      public void completed() {
        handleCompleted(null);
      }

      @Override
      public void completedWithException(Throwable t) {
        handleCompleted(t);
      }

      private void handleCompleted(@Nullable Throwable t) {
        // If the completed call is from the main thread, it means the calling to user class has been returned,
        // since it's the last thing that Spark main method would do.
        if (Thread.currentThread() == mainThread) {
          mainThread.setContextClassLoader(oldClassLoader);
        }

        if (driverService instanceof SparkProgramCompletion) {
          SparkProgramCompletion delegate = (SparkProgramCompletion) driverService;
          if (t == null) {
            delegate.completed();
          } else {
            delegate.completedWithException(t);
          }
        } else {
          driverService.stop();
        }

        // Wait for the spark execution context to stop to make sure everything related
        // to the spark execution is shutdown properly before returning.
        // When this method returns, the driver's main thread returns, which will have system services shutdown
        // and process terminated. If system services stopped concurrently with the execution context, we may have some
        // logs and tasks missing.
        if (!Uninterruptibles.awaitUninterruptibly(secStopLatch, 30, TimeUnit.SECONDS)) {
          LOG.warn("Failed to stop the spark execution context in 30 seconds");
        }
      }
    };
  }

  /**
   * Adds a {@link EventLoggingListener} to the Spark context.
   *
   * @param runtimeContext the {@link SparkRuntimeContext} for connecting to CDAP services
   * @return A {@link Closeable} which should be called when the Spark application completed
   */
  public static Closeable addEventLoggingListener(SparkRuntimeContext runtimeContext) throws IOException {
    // If upload event logs is not enabled, just return a dummy closeable
    if (!runtimeContext.getCConfiguration().getBoolean(Constants.AppFabric.SPARK_EVENT_LOGS_ENABLED)) {
      return () -> { };
    }

    SparkConf sparkConf = new SparkConf();
    sparkConf.set("spark.eventLog.enabled", Boolean.toString(true));
    sparkConf.set("spark.eventLog.compress", Boolean.toString(true));

    ProgramRunId programRunId = runtimeContext.getProgramRunId();

    File eventLogDir = Files.createTempDirectory("spark-events").toFile();
    LOG.debug("Spark events log directory for {} is {}", programRunId, eventLogDir);

    // EventLoggingListener is Scala package private[spark] class.
    // However, since JVM bytecode doesn't really have this type of cross package private support, the class
    // is actually public class. This is why we can access it in Java code.
    EventLoggingListener listener = new EventLoggingListener(Long.toString(System.currentTimeMillis()), Option.empty(),
                                                             eventLogDir.toURI(), sparkConf) {
      @Override
      public void onApplicationStart(SparkListenerApplicationStart event) {
        // Rewrite the application start event with identifiable names based on the program run id such that
        // it can be tied back to the program run.
        // We use reflection to modify the field instead of creating a new event instance due to
        // non-binary compatible Spark API change between Spark 2 and Spark 3.
        try {
          Field appName = event.getClass().getDeclaredField("appName");
          appName.setAccessible(true);
          Field appId = event.getClass().getDeclaredField("appId");
          appId.setAccessible(true);

          appName.set(event, String.format("%s.%s.%s.%s",
                                           programRunId.getNamespace(),
                                           programRunId.getApplication(),
                                           programRunId.getType().getPrettyName().toLowerCase(),
                                           programRunId.getProgram()));

          appId.set(event, Option.apply(programRunId.getRun()));
        } catch (Exception e) {
          LOG.warn("Failed to alter application start event", e);
        }

        super.onApplicationStart(event);
      }
    };
    listener.start();
    SparkRuntimeEnv.addSparkListener(listener);

    return () -> {
      listener.stop();
      uploadEventLogs(eventLogDir, runtimeContext);
      DirUtils.deleteDirectoryContents(eventLogDir);
    };
  }

  /**
   * Creates a {@link Service} that runs in the Spark driver process for lifecycle management. In distributed mode,
   * it will be the {@link SparkDriverService} that also responsible for heartbeating and delegation token updates.
   */
  private static Service createSparkDriverService(SparkRuntimeContext runtimeContext) {
    String executorServiceURI = System.getenv(CDAP_SPARK_EXECUTION_SERVICE_URI);
    if (executorServiceURI != null) {
      // Creates the SparkDriverService in distributed mode for heartbeating and tokens update
      return new SparkDriverService(URI.create(executorServiceURI), runtimeContext);
    }

    // In local mode, just create a no-op service for state transition.
    return new AbstractService() {
      @Override
      protected void doStart() {
        notifyStarted();
      }

      @Override
      protected void doStop() {
        notifyStopped();
      }
    };
  }

  /**
   * Uploads the spark event logs through the runtime service.
   */
  private static void uploadEventLogs(File eventLogDir, SparkRuntimeContext runtimeContext) throws IOException {

    ProgramRunId programRunId = runtimeContext.getProgramRunId();

    // Find the event file to upload. There should only be one for the current application.
    File eventFile = Optional.ofNullable(eventLogDir.listFiles())
      .map(Arrays::stream)
      .flatMap(Stream::findFirst)
      .orElse(null);

    if (eventFile == null) {
      // This shouldn't happen. If it does for some reason, just log and return.
      LOG.warn("Cannot find event logs file in {} for program run {}", eventLogDir, programRunId);
      return;
    }

    RuntimeClient runtimeClient = new RuntimeClient(runtimeContext.getCConfiguration(),
                                                    runtimeContext.getRemoteClientFactory());
    Retries.runWithRetries(() -> runtimeClient.uploadSparkEventLogs(programRunId, eventFile),
                           RetryStrategies.fromConfiguration(runtimeContext.getCConfiguration(), "spark."));
    LOG.debug("Uploaded event logs file {} for program run {}", eventFile, programRunId);
  }

  /**
   * Appends value of cdap.spark.pyFiles to the original paths list.
   * This function is used to simplify code generation in
   * {@link SparkClassRewriter#rewritePythonWorkerFactory(java.io.InputStream)}.
   * @param paths original path list
   * @return new path list with cdap.spark.pyFiles added (if any)
   */
  public static String appendPyFiles(String paths) {
    String pyFiles = SparkRuntimeEnv.getProperty("cdap.spark.pyFiles");
    return pyFiles == null ? paths : paths + File.pathSeparator + pyFiles;
  }
}
