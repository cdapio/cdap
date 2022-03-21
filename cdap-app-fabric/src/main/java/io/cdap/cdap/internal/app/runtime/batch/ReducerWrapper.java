/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.batch;

import com.google.common.base.Throwables;
import io.cdap.cdap.api.ProgramLifecycle;
import io.cdap.cdap.api.RuntimeContext;
import io.cdap.cdap.common.lang.ClassLoaders;
import io.cdap.cdap.common.lang.PropertyFieldSetter;
import io.cdap.cdap.common.lang.WeakReferenceDelegatorClassLoader;
import io.cdap.cdap.internal.app.runtime.DataSetFieldSetter;
import io.cdap.cdap.internal.app.runtime.MetricsFieldSetter;
import io.cdap.cdap.internal.lang.Reflections;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.reduce.WrappedReducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Wraps user-defined implementation of {@link Reducer} class which allows perform extra configuration.
 */
public class ReducerWrapper extends Reducer {

  private static final Logger LOG = LoggerFactory.getLogger(ReducerWrapper.class);
  private static final String ATTR_REDUCER_CLASS = "c.reducer.class";

  /**
   * Wraps the reducer defined in the job with this {@link ReducerWrapper} if it is defined.
   * @param job The MapReduce job
   */
  public static void wrap(Job job) {
    // NOTE: we don't use job.getReducerClass() as we don't need to load user class here
    Configuration conf = job.getConfiguration();
    String reducerClass = conf.get(MRJobConfig.REDUCE_CLASS_ATTR);
    if (reducerClass != null) {
      conf.set(ReducerWrapper.ATTR_REDUCER_CLASS, reducerClass);
      job.setReducerClass(ReducerWrapper.class);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void run(Context context) throws IOException, InterruptedException {
    MapReduceClassLoader classLoader = MapReduceClassLoader.getFromConfiguration(context.getConfiguration());
    ClassLoader weakReferenceClassLoader = new WeakReferenceDelegatorClassLoader(classLoader);

    BasicMapReduceTaskContext basicMapReduceContext = classLoader.getTaskContextProvider().get(context);
    long metricsReportInterval = basicMapReduceContext.getMetricsReportIntervalMillis();
    final ReduceTaskMetricsWriter reduceTaskMetricsWriter = new ReduceTaskMetricsWriter(
      basicMapReduceContext.getProgramMetrics(), context);

    // this is a hook for periodic flushing of changes buffered by datasets (to avoid OOME)
    WrappedReducer.Context flushingContext = createAutoFlushingContext(context, basicMapReduceContext,
                                                                       reduceTaskMetricsWriter);
    basicMapReduceContext.setHadoopContext(flushingContext);

    String userReducer = context.getConfiguration().get(ATTR_REDUCER_CLASS);
    ClassLoader programClassLoader = classLoader.getProgramClassLoader();
    Reducer delegate = createReducerInstance(programClassLoader, userReducer);

    // injecting runtime components, like datasets, etc.
    try {
      Reflections.visit(delegate, delegate.getClass(),
                        new PropertyFieldSetter(basicMapReduceContext.getSpecification().getProperties()),
                        new MetricsFieldSetter(basicMapReduceContext.getMetrics()),
                        new DataSetFieldSetter(basicMapReduceContext));
    } catch (Throwable t) {
      LOG.error("Failed to inject fields to {}.", delegate.getClass(), t);
      throw Throwables.propagate(t);
    }

    ClassLoader oldClassLoader;
    if (delegate instanceof ProgramLifecycle) {
      oldClassLoader = ClassLoaders.setContextClassLoader(weakReferenceClassLoader);
      try {
        ((ProgramLifecycle) delegate).initialize(new MapReduceLifecycleContext(basicMapReduceContext));
      } catch (Exception e) {
        LOG.error("Failed to initialize reducer with {}", basicMapReduceContext, e);
        throw Throwables.propagate(e);
      } finally {
        ClassLoaders.setContextClassLoader(oldClassLoader);
      }
    }

    oldClassLoader = ClassLoaders.setContextClassLoader(weakReferenceClassLoader);
    try {
      delegate.run(flushingContext);
    } finally {
      ClassLoaders.setContextClassLoader(oldClassLoader);
    }

    // transaction is not finished, but we want all operations to be dispatched (some could be buffered in
    // memory by tx agent)
    try {
      basicMapReduceContext.flushOperations();
    } catch (Exception e) {
      LOG.error("Failed to flush operations at the end of reducer of " + basicMapReduceContext, e);
      throw Throwables.propagate(e);
    }

    // Close all writers created by MultipleOutputs
    basicMapReduceContext.closeMultiOutputs();

    if (delegate instanceof ProgramLifecycle) {
      oldClassLoader = ClassLoaders.setContextClassLoader(weakReferenceClassLoader);
      try {
        ((ProgramLifecycle<? extends RuntimeContext>) delegate).destroy();
      } catch (Exception e) {
        LOG.error("Error during destroy of reducer {}", basicMapReduceContext, e);
        // Do nothing, try to finish
      } finally {
        ClassLoaders.setContextClassLoader(oldClassLoader);
      }
    }

    reduceTaskMetricsWriter.reportMetrics();
  }

  private WrappedReducer.Context createAutoFlushingContext(final Context context,
                                                           final BasicMapReduceTaskContext basicMapReduceContext,
                                                           final ReduceTaskMetricsWriter metricsWriter) {
    // NOTE: we will change auto-flush to take into account size of buffered data, so no need to do/test a lot with
    //       current approach
    final int flushFreq = context.getConfiguration().getInt("c.reducer.flush.freq", 10000);
    final long reportIntervalInMillis = basicMapReduceContext.getMetricsReportIntervalMillis();

    @SuppressWarnings("unchecked")
    WrappedReducer.Context flushingContext = new WrappedReducer().new Context(context) {
      private int processedRecords;
      private long nextTimeToReportMetrics;

      @Override
      public boolean nextKeyValue() throws IOException, InterruptedException {
        boolean result = super.nextKey();
        if (++processedRecords > flushFreq) {
          try {
            LOG.trace("Flushing dataset operations...");
            basicMapReduceContext.flushOperations();
          } catch (Exception e) {
            LOG.error("Failed to persist changes", e);
            throw Throwables.propagate(e);
          }
          processedRecords = 0;
        }

        if (System.currentTimeMillis() >= nextTimeToReportMetrics) {
          metricsWriter.reportMetrics();
          nextTimeToReportMetrics = System.currentTimeMillis() + reportIntervalInMillis;
        }
        return result;
      }
    };
    return flushingContext;
  }

  private Reducer createReducerInstance(ClassLoader classLoader, String userReducer) {
    try {
      return (Reducer) classLoader.loadClass(userReducer).newInstance();
    } catch (Exception e) {
      LOG.error("Failed to create instance of the user-defined Reducer class: " + userReducer);
      throw Throwables.propagate(e);
    }
  }
}
