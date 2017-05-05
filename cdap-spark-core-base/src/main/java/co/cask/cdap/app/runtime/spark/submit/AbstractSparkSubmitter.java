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
import co.cask.cdap.app.runtime.spark.SparkMainWrapper;
import co.cask.cdap.app.runtime.spark.SparkRuntimeContext;
import co.cask.cdap.app.runtime.spark.SparkRuntimeUtils;
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
import org.apache.twill.common.Cancellable;
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
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

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
    final SparkJobFuture<V> resultFuture = new SparkJobFuture<V>(runtimeContext) {
      @Override
      protected void cancelTask() {
        // Try to shutdown the running spark job.
        triggerShutdown();

        // Wait for the Spark-Submit returns
        Uninterruptibles.awaitUninterruptibly(completion);
      }
    };

    // Submit the Spark job
    executor.submit(new Runnable() {
      @Override
      public void run() {
        List<String> extraArgs = beforeSubmit();
        try {
          String[] submitArgs = Iterables.toArray(Iterables.concat(args, extraArgs), String.class);
          submit(runtimeContext, submitArgs);
          onCompleted(true);
          resultFuture.set(result);
        } catch (Throwable t) {
          onCompleted(false);
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
  protected abstract void triggerShutdown();

  protected List<String> beforeSubmit() {
    return Collections.emptyList();
  }

  protected void onCompleted(boolean succeeded) {
    // no-op
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
  private void submit(SparkRuntimeContext runtimeContext, String[] args) {
    Cancellable cancellable = SparkRuntimeUtils.setContextClassLoader(new SparkClassLoader(runtimeContext));
    try {
      LOG.debug("Calling SparkSubmit for {} {}: {}",
                runtimeContext.getProgram().getId(), runtimeContext.getRunId(), Arrays.toString(args));
      // Explicitly set the SPARK_SUBMIT property as it is no longer set on the System properties by the SparkSubmit
      // after the class rewrite. This property only control logging of a warning when submitting the Spark job,
      // hence it's harmless to just leave it there.
      System.setProperty("SPARK_SUBMIT", "true");
      SparkSubmit.main(args);
      LOG.debug("SparkSubmit returned for {} {}", runtimeContext.getProgram().getId(), runtimeContext.getRunId());
    } finally {
      cancellable.cancel();
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
      .add("--" + SparkMainWrapper.ARG_USER_CLASS() + "=" + spec.getMainClassName())
      .build();
  }

  /**
   * A {@link Future} implementation for representing a Spark job execution, which allows cancelling the job through
   * the {@link #cancel(boolean)} method. When the job execution is completed, the {@link #set(Object)} should be
   * called for successful execution, or call the {@link #setException(Throwable)} for failure. To terminate the
   * job execution while it is running, call the {@link #cancel(boolean)} method. Sub-classes should override the
   * {@link #cancelTask()} method for cancelling the execution and the state of this {@link Future} will change
   * to cancelled after the {@link #cancelTask()} call returns.
   *
   * @param <V> type of object returned by the {@link #get()} method.
   */
  private abstract static class SparkJobFuture<V> extends AbstractFuture<V> {

    private static final Logger LOG = LoggerFactory.getLogger(SparkJobFuture.class);
    private final AtomicBoolean done;
    private final SparkRuntimeContext context;

    protected SparkJobFuture(SparkRuntimeContext context) {
      this.done = new AtomicBoolean();
      this.context = context;
    }

    @Override
    protected boolean set(V value) {
      if (done.compareAndSet(false, true)) {
        return super.set(value);
      }
      return false;
    }

    @Override
    protected boolean setException(Throwable throwable) {
      if (done.compareAndSet(false, true)) {
        return super.setException(throwable);
      }
      return false;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      if (!done.compareAndSet(false, true)) {
        return false;
      }

      try {
        cancelTask();
        return super.cancel(mayInterruptIfRunning);
      } catch (Throwable t) {
        // Only log and reset state, but not propagate since Future.cancel() doesn't expect exception to be thrown.
        LOG.warn("Failed to cancel Spark execution for {}.", context, t);
        done.set(false);
        return false;
      }
    }


    @Override
    protected final void interruptTask() {
      // Final it so that it cannot be overridden. This method gets call after the Future state changed
      // to cancel, hence cannot have the caller block until cancellation is done.
    }

    /**
     * Will be called to cancel an executing task. Sub-class can override this method to provide
     * custom cancellation logic. This method will be called before the future changed to cancelled state.
     */
    protected void cancelTask() {
      // no-op
    }
  }
}
