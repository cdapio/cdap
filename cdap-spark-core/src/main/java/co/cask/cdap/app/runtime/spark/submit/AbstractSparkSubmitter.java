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

package co.cask.cdap.app.runtime.spark.submit;


import co.cask.cdap.api.spark.SparkSpecification;
import co.cask.cdap.app.runtime.spark.SparkClassLoader;
import co.cask.cdap.app.runtime.spark.SparkContextCache;
import co.cask.cdap.app.runtime.spark.SparkExecutionContextFactory;
import co.cask.cdap.app.runtime.spark.SparkMainWrapper;
import co.cask.cdap.app.runtime.spark.SparkRuntimeContext;
import co.cask.cdap.common.lang.ClassLoaders;
import co.cask.cdap.common.lang.WeakReferenceDelegatorClassLoader;
import co.cask.cdap.internal.app.runtime.distributed.LocalizeResource;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.spark.deploy.SparkSubmit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import javax.annotation.Nullable;

/**
 * Provides common implementation for different {@link SparkSubmitter}.
 */
public abstract class AbstractSparkSubmitter implements SparkSubmitter {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractSparkSubmitter.class);

  // Filter for getting archive resources only
  private static final Predicate<LocalizeResource> ARCHIVE_FILTER = new Predicate<LocalizeResource>() {
    @Override
    public boolean apply(LocalizeResource input) {
      return input.isArchive();
    }
  };

  // Transforms LocalizeResource to URI string
  private static final Function<LocalizeResource, String> RESOURCE_TO_PATH = new Function<LocalizeResource, String>() {
    @Override
    public String apply(LocalizeResource input) {
      return input.getURI().toString();
    }
  };

  @Override
  public final <V> ListenableFuture<V> submit(final SparkRuntimeContext runtimeContext,
                                              final SparkExecutionContextFactory contextFactory,
                                              Map<String, String> configs, List<LocalizeResource> resources,
                                              File jobJar, final V result) {
    final SparkSpecification spec = runtimeContext.getSparkSpecification();

    final List<String> args = createSubmitArguments(spec, configs, resources, jobJar);

    // Spark submit is called from this executor
    // Use an executor to simplify logic that is needed to interrupt the running thread on stopping
    final ExecutorService executor = Executors.newSingleThreadExecutor(new ThreadFactory() {
      @Override
      public Thread newThread(Runnable r) {
        return new Thread(r, "spark-submitter-" + spec.getName() + "-" + runtimeContext.getRunId());
      }
    });

    // Latch for the Spark job completion
    final CountDownLatch completion = new CountDownLatch(1);
    final SparkJobFuture<V> resultFuture = new SparkJobFuture<V>() {
      @Override
      protected void interruptTask() {
        // Try to shutdown the running spark job.
        triggerShutdown();

        // Interrupt the executing thread as well in case it is blocking in somewhere.
        executor.shutdownNow();
        Uninterruptibles.awaitUninterruptibly(completion);
      }
    };

    // Submit the Spark job
    executor.submit(new Runnable() {
      @Override
      public void run() {
        try {
          submit(runtimeContext, contextFactory, args.toArray(new String[args.size()]));
          resultFuture.set(result);
        } catch (Throwable t) {
          resultFuture.setException(t);
        } finally {
          completion.countDown();
        }
      }
    });
    // Shutdown the executor right after submit since the thread is only used for one submission.
    executor.shutdown();
    return resultFuture;
  }

  /**
   * Returns the value for the {@code --master} argument for the Spark submission.
   */
  protected abstract String getMaster(Map<String, String> configs);

  /**
   * Invoked for stopping the Spark job explicitly.
   */
  protected void triggerShutdown() {
    // Try to get the SparkContext and call stop on it.
    try {
      SparkContextCache.stop();
    } catch (Throwable t) {
      // Don't propagate the exception.
      LOG.error("Exception while calling SparkContext.stop()", t);
    }
  }

  /**
   * Returns configs that are specific to the submission context.
   */
  protected Map<String, String> getSubmitConf() {
    return Collections.emptyMap();
  }

  /**
   * Submits the Spark job using {@link SparkSubmit}.
   *
   * @param runtimeContext context representing the Spark program
   * @param args arguments for the {@link SparkSubmit#main(String[])} method.
   */
  protected void submit(SparkRuntimeContext runtimeContext,
                        SparkExecutionContextFactory contextFactory, String[] args) {
    SparkClassLoader sparkClassLoader = new SparkClassLoader(runtimeContext, contextFactory);
    ClassLoader oldClassLoader = ClassLoaders.setContextClassLoader(
      new WeakReferenceDelegatorClassLoader(sparkClassLoader));

    try {
      LOG.debug("Calling SparkSubmit for {} {}: {}",
                runtimeContext.getProgram().getId(), runtimeContext.getRunId(), Arrays.toString(args));
      SparkSubmit.main(args);
      LOG.debug("SparkSubmit returned for {} {}", runtimeContext.getProgram().getId(), runtimeContext.getRunId());
    } finally {
      ClassLoaders.setContextClassLoader(oldClassLoader);
    }
  }

  /**
   * Creates the list of arguments that will be used for calling {@link SparkSubmit#main(String[])}.
   *
   * @param spec the {@link SparkSpecification} of the program
   * @param configs set of Spark configurations
   * @param resources list of resources that needs to be localized to Spark containers
   * @param jobJar the job jar file for Spark
   * @return a list of arguments
   */
  private List<String> createSubmitArguments(SparkSpecification spec, Map<String, String> configs,
                                             List<LocalizeResource> resources, File jobJar) {
    ImmutableList.Builder<String> builder = ImmutableList.builder();

    builder.add("--master").add(getMaster(configs));
    builder.add("--class").add(SparkMainWrapper.class.getName());
    builder.add("--conf").add("spark.app.name=" + spec.getName());

    for (Map.Entry<String, String> entry : configs.entrySet()) {
      builder.add("--conf").add(entry.getKey() + "=" + entry.getValue());
    }

    for (Map.Entry<String, String> entry : getSubmitConf().entrySet()) {
      builder.add("--conf").add(entry.getKey() + "=" + entry.getValue());
    }

    String archives = Joiner.on(',')
      .join(Iterables.transform(Iterables.filter(resources, ARCHIVE_FILTER), RESOURCE_TO_PATH));
    String files = Joiner.on(',')
      .join(Iterables.transform(Iterables.filter(resources, Predicates.not(ARCHIVE_FILTER)), RESOURCE_TO_PATH));

    if (!archives.isEmpty()) {
      builder.add("--archives").add(archives);
    }
    if (!files.isEmpty()) {
      builder.add("--files").add(files);
    }

    return builder
      .add(jobJar.getAbsolutePath())
      .add(spec.getMainClassName())
      .build();
  }

  private abstract static class SparkJobFuture<V> extends AbstractFuture<V> {

    @Override
    public boolean set(@Nullable V value) {
      return super.set(value);
    }

    @Override
    public boolean setException(Throwable throwable) {
      return super.setException(throwable);
    }
  }
}
