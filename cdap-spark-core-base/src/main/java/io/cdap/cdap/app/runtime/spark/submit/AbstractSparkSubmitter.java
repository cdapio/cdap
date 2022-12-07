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

package io.cdap.cdap.app.runtime.spark.submit;


import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicates;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.common.util.concurrent.Uninterruptibles;
import io.cdap.cdap.api.spark.SparkSpecification;
import io.cdap.cdap.app.runtime.spark.SparkMainWrapper;
import io.cdap.cdap.app.runtime.spark.SparkRuntimeContext;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.lang.ClassLoaders;
import io.cdap.cdap.internal.app.runtime.distributed.LocalizeResource;
import org.apache.spark.deploy.SparkSubmit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import javax.annotation.Nullable;

/**
 * Provides common implementation for different {@link SparkSubmitter}.
 */
public abstract class AbstractSparkSubmitter implements SparkSubmitter {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractSparkSubmitter.class);

  // Transforms LocalizeResource to URI string
  private static final Function<LocalizeResource, String> RESOURCE_TO_PATH = input -> input.getURI().toString();

  @Override
  public final <V> SparkJobFuture<V> submit(SparkRuntimeContext runtimeContext,
                                            Map<String, String> configs, List<LocalizeResource> resources,
                                            URI jobFile, final V result) throws Exception {
    SparkSpecification spec = runtimeContext.getSparkSpecification();

    List<String> args = createSubmitArguments(runtimeContext, configs, resources, jobFile);

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
        List<String> extraArgs = beforeSubmit();
        String[] submitArgs = Iterables.toArray(Iterables.concat(args, extraArgs), String.class);
        submit(runtimeContext, submitArgs);
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
   * Add the {@code --master} argument for the Spark submission.
   * @throws Exception if there is error while getting master ip address from spark config
   */
  protected abstract void addMaster(Map<String, String> configs, ImmutableList.Builder<String> argBuilder)
    throws Exception;

  /**
   * Invoked for stopping the Spark job explicitly.
   */
  protected abstract void triggerShutdown(long timeout, TimeUnit timeoutTimeUnit);

  /**
   * Called before submitting the Spark job.
   *
   * @return list of extra arguments to pass to {@link SparkSubmit}.
   */
  protected List<String> beforeSubmit() throws Exception {
    return Collections.emptyList();
  }

  /**
   * Called when the Spark program finished.
   *
   * @param succeeded {@code true} to indicate the program completed successfully as reported by SparkSubmit.
   */
  protected void onCompleted(boolean succeeded) {
    // no-op
  }

  /**
   * Returns configs that are specific to the submission context.
   * @throws Exception if there is error while generating submit conf.
   */
  protected Map<String, String> generateSubmitConf() throws Exception {
    return Collections.emptyMap();
  }

  /**
   * Returns iterable of archives from list of localize resources.
   */
  protected Iterable<LocalizeResource> getArchives(List<LocalizeResource> localizeResources) {
    return Iterables.filter(localizeResources, LocalizeResource::isArchive);
  }

  /**
   * Returns iterable of archives from list of localize resources.
   */
  protected Iterable<LocalizeResource> getFiles(List<LocalizeResource> localizeResources) {
    return Iterables.filter(localizeResources, Predicates.not(LocalizeResource::isArchive));
  }

  /**
   * Returns job file for spark.
   * @throws Exception if there is error getting job jar file
   */
  @Nullable
  protected URI getJobFile() throws Exception {
    return null;
  }

  /**
   * Returns true if spark driver has succeeded.
   */
  protected boolean waitForFinish() throws Exception {
    return true;
  }

  /**
   * Submits the Spark job using {@link SparkSubmit}.
   *
   * @param runtimeContext context representing the Spark program
   * @param args arguments for the {@link SparkSubmit#main(String[])} method.
   */
  private void submit(SparkRuntimeContext runtimeContext, String[] args) {
    ClassLoader oldClassLoader = ClassLoaders.setContextClassLoader(runtimeContext.getProgramInvocationClassLoader());
    try {
      LOG.debug("Calling SparkSubmit for {} {}: {}",
                runtimeContext.getProgram().getId(), runtimeContext.getRunId(), Arrays.toString(args));
      LOG.info("ashau - Calling SparkSubmit for {} {}: {}",
                runtimeContext.getProgram().getId(), runtimeContext.getRunId(), Arrays.toString(args), new Exception());
      // Explicitly set the SPARK_SUBMIT property as it is no longer set on the System properties by the SparkSubmit
      // after the class rewrite. This property only control logging of a warning when submitting the Spark job,
      // hence it's harmless to just leave it there.
      System.setProperty("SPARK_SUBMIT", "true");
      SparkSubmit.main(args);
      LOG.debug("SparkSubmit returned for {} {}", runtimeContext.getProgram().getId(), runtimeContext.getRunId());
    } finally {
      ClassLoaders.setContextClassLoader(oldClassLoader);
    }
  }

  /**
   * Creates the list of arguments that will be used for calling {@link SparkSubmit#main(String[])}.
   *
   * @param runtimeContext the {@link SparkRuntimeContext} for the spark program
   * @param configs set of Spark configurations
   * @param resources list of resources that needs to be localized to Spark containers
   * @param jobFile the job file for Spark
   * @return a list of arguments
   * @throws Exception if there is error while creating submit arguments
   */
  private List<String> createSubmitArguments(SparkRuntimeContext runtimeContext, Map<String, String> configs,
                                             List<LocalizeResource> resources, URI jobFile) throws Exception {
    SparkSpecification spec = runtimeContext.getSparkSpecification();

    ImmutableList.Builder<String> builder = ImmutableList.builder();
    Iterable<LocalizeResource> archivesIterable = getArchives(resources);
    Iterable<LocalizeResource> filesIterable = getFiles(resources);

    addMaster(configs, builder);
    builder.add("--conf").add("spark.app.name=" + spec.getName());

    configs.putAll(generateSubmitConf());
    BiConsumer<String, String> confAdder = (k, v) -> builder.add("--conf").add(k + "=" + v);
    configs.forEach(confAdder);

    String archives = Joiner.on(',').join(Iterables.transform(archivesIterable, RESOURCE_TO_PATH));
    String files = Joiner.on(',').join(Iterables.transform(filesIterable, RESOURCE_TO_PATH));

    if (!Strings.isNullOrEmpty(archives)) {
      builder.add("--archives").add(archives);
    }
    if (!Strings.isNullOrEmpty(files)) {
      builder.add("--files").add(files);
    }

    URI newJobFile = getJobFile();
    if (newJobFile != null) {
      jobFile = newJobFile;
    }

    boolean isPySpark = jobFile.getPath().endsWith(".py");
    if (isPySpark) {
      // For python, add extra py library files
      String pyFiles = configs.get("spark.submit.pyFiles");
      if (pyFiles != null) {
        builder.add("--py-files").add(pyFiles);
      }
    } else {
      builder.add("--class").add(SparkMainWrapper.class.getName());
    }

    if ("file".equals(jobFile.getScheme())) {
      builder.add(jobFile.getPath());
    } else {
      builder.add(jobFile.toString());
    }

    if (!isPySpark) {
      // Add extra arguments for easily identifying the program from command line.
      // Arguments to user program is always coming from the runtime arguments.
      builder.add("--cdap.spark.program=" + runtimeContext.getProgramRunId().toString());
      builder.add("--cdap.user.main.class=" + spec.getMainClassName());
    }

    return builder.build();
  }
}
