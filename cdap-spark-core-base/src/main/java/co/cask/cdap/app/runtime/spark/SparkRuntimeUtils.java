/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.app.runtime.spark;

import co.cask.cdap.api.spark.SparkExecutionContext;
import co.cask.cdap.app.runtime.spark.classloader.SparkRunnerClassLoader;
import co.cask.cdap.app.runtime.spark.distributed.SparkDriverService;
import co.cask.cdap.common.lang.ClassLoaders;
import co.cask.cdap.common.lang.jar.BundleJarUtil;
import com.google.common.io.OutputSupplier;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.common.util.concurrent.Uninterruptibles;
import com.google.gson.Gson;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.DStreamGraph;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.Threads;
import org.apache.twill.internal.ServiceListenerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.collection.parallel.TaskSupport;
import scala.collection.parallel.ThreadPoolTaskSupport;
import scala.collection.parallel.mutable.ParArray;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * Util class for common functions needed for Spark implementation.
 */
public final class SparkRuntimeUtils {

  public static final String CDAP_SPARK_EXECUTION_SERVICE_URI = "CDAP_SPARK_EXECUTION_SERVICE_URI";

  private static final String LOCALIZED_RESOURCES = "spark.cdap.localized.resources";
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
      BundleJarUtil.createArchive(confDir, new OutputSupplier<ZipOutputStream>() {
        @Override
        public ZipOutputStream getOutput() throws IOException {
          ZipOutputStream zipOutput = new ZipOutputStream(new FileOutputStream(zipFile));
          zipOutput.putNextEntry(new ZipEntry(propertiesEntryName));
          properties.store(zipOutput, "Spark configuration.");
          zipOutput.closeEntry();

          return zipOutput;
        }
      });
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
   * @return a {@link Cancellable} for releasing resources.
   */
  public static Cancellable initSparkMain() {
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

    String executorServiceURI = System.getenv(CDAP_SPARK_EXECUTION_SERVICE_URI);
    final Service driverService;
    if (executorServiceURI != null) {
      // Creates the SparkDriverService in distributed mode for heartbeating and tokens update
      driverService = new SparkDriverService(URI.create(executorServiceURI), runtimeContext);
    } else {
      // In local mode, just create a no-op service for state transition.
      driverService = new AbstractService() {
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

    // Watch for stopping of the driver service.
    // It can happen when a user program finished such that the Cancellable.cancel() returned by this method is called,
    // or it can happen when it received a stop command (distributed mode) in the SparkDriverService via heartbeat.
    // In local mode, the LocalSparkSubmitter calls the Cancellable.cancel() returned by this method directly
    // (via SparkMainWraper).
    // We use a service listener so that it can handle all cases.
    final CountDownLatch mainThreadCallLatch = new CountDownLatch(1);
    driverService.addListener(new ServiceListenerAdapter() {

      @Override
      public void terminated(Service.State from) {
        handleStopped();
      }

      @Override
      public void failed(Service.State from, Throwable failure) {
        handleStopped();
      }

      private void handleStopped() {
        // Avoid interrupt/join on the current thread
        if (Thread.currentThread() != mainThread) {
          mainThread.interrupt();
          // If it is spark streaming, wait for the user class call returns from the main thread.
          if (SparkRuntimeEnv.getStreamingContext().isDefined()) {
            Uninterruptibles.awaitUninterruptibly(mainThreadCallLatch);
          }
        }

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
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    driverService.startAndWait();
    return new Cancellable() {
      @Override
      public void cancel() {
        // If the cancel call is from the main thread, it means the calling to user class has been returned,
        // since it's the last thing that Spark main methhod would do.
        if (Thread.currentThread() == mainThread) {
          mainThreadCallLatch.countDown();
          mainThread.setContextClassLoader(oldClassLoader);
        }
        driverService.stopAndWait();
      }
    };
  }
}
