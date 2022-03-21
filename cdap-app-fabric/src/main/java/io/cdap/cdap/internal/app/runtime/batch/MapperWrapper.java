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

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import io.cdap.cdap.api.ProgramLifecycle;
import io.cdap.cdap.api.RuntimeContext;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.lang.ClassLoaders;
import io.cdap.cdap.common.lang.PropertyFieldSetter;
import io.cdap.cdap.common.lang.WeakReferenceDelegatorClassLoader;
import io.cdap.cdap.common.logging.Loggers;
import io.cdap.cdap.internal.app.runtime.DataSetFieldSetter;
import io.cdap.cdap.internal.app.runtime.MetricsFieldSetter;
import io.cdap.cdap.internal.app.runtime.batch.dataset.input.InputContexts;
import io.cdap.cdap.internal.app.runtime.batch.dataset.input.MultiInputTaggedSplit;
import io.cdap.cdap.internal.app.runtime.batch.dataset.input.TaggedInputSplit;
import io.cdap.cdap.internal.lang.Reflections;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.map.WrappedMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Wraps user-defined implementation of {@link Mapper} class which allows perform extra configuration.
 */
public class MapperWrapper extends Mapper {

  private static final Logger LOG = LoggerFactory.getLogger(MapperWrapper.class);
  private static final Logger USERLOG = Loggers.mdcWrapper(LOG, Constants.Logging.EVENT_TYPE_TAG,
                                                           Constants.Logging.USER_LOG_TAG_VALUE);
  private static final String ATTR_MAPPER_CLASS = "c.mapper.class";

  /**
   * Wraps the mapper defined in the job with this {@link MapperWrapper} if it is defined.
   * @param job The MapReduce job
   */
  public static void wrap(Job job) {
    // NOTE: we don't use job.getMapperClass() as we don't need to load user class here
    Configuration conf = job.getConfiguration();
    String mapClass = conf.get(MRJobConfig.MAP_CLASS_ATTR, Mapper.class.getName());
    conf.set(MapperWrapper.ATTR_MAPPER_CLASS, mapClass);
    job.setMapperClass(MapperWrapper.class);
  }

  /**
   * Retrieves the class name of the wrapped mapper class from a Job's configuration.
   *
   * @param conf The conf from which to get the wrapped class.
   * @return the class name of the wrapped Mapper class
   */
  public static String getWrappedMapper(Configuration conf) {
    String wrappedMapperClassName = conf.get(MapperWrapper.ATTR_MAPPER_CLASS);
    Preconditions.checkNotNull(wrappedMapperClassName, "Wrapped mapper class could not be found.");
    return wrappedMapperClassName;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void run(Context context) throws IOException, InterruptedException {
    MapReduceClassLoader classLoader = MapReduceClassLoader.getFromConfiguration(context.getConfiguration());
    ClassLoader weakReferenceClassLoader = new WeakReferenceDelegatorClassLoader(classLoader);

    BasicMapReduceTaskContext basicMapReduceContext = classLoader.getTaskContextProvider().get(context);
    String program = basicMapReduceContext.getProgramName();

    final MapTaskMetricsWriter mapTaskMetricsWriter = new MapTaskMetricsWriter(
      basicMapReduceContext.getProgramMetrics(), context);
    // this is a hook for periodic flushing of changes buffered by datasets (to avoid OOME)
    WrappedMapper.Context flushingContext = createAutoFlushingContext(context, basicMapReduceContext,
                                                                      mapTaskMetricsWriter);

    basicMapReduceContext.setHadoopContext(flushingContext);
    InputSplit inputSplit = context.getInputSplit();
    if (inputSplit instanceof MultiInputTaggedSplit) {
      basicMapReduceContext.setInputContext(InputContexts.create((MultiInputTaggedSplit) inputSplit));
    }

    ClassLoader programClassLoader = classLoader.getProgramClassLoader();
    Mapper delegate = createMapperInstance(programClassLoader, getWrappedMapper(context.getConfiguration()), context,
                                           program);

    // injecting runtime components, like datasets, etc.
    try {
      Reflections.visit(delegate, delegate.getClass(),
                        new PropertyFieldSetter(basicMapReduceContext.getSpecification().getProperties()),
                        new MetricsFieldSetter(basicMapReduceContext.getMetrics()),
                        new DataSetFieldSetter(basicMapReduceContext));
    } catch (Throwable t) {
      Throwable rootCause = Throwables.getRootCause(t);
      USERLOG.error("Failed to initialize program '{}' with error: {}. Please check the system logs for more details.",
                    program, rootCause.getMessage(), rootCause);
      throw new IOException(String.format("Failed to inject fields to %s", delegate.getClass()), t);
    }

    ClassLoader oldClassLoader;
    if (delegate instanceof ProgramLifecycle) {
      oldClassLoader = ClassLoaders.setContextClassLoader(weakReferenceClassLoader);
      try {
        ((ProgramLifecycle) delegate).initialize(new MapReduceLifecycleContext(basicMapReduceContext));
      } catch (Exception e) {
        Throwable rootCause = Throwables.getRootCause(e);
        USERLOG.error("Failed to initialize program '{}' with error: {}. Please check the system logs for more " +
                        "details.", program, rootCause.getMessage(), rootCause);
        throw new IOException(String.format("Failed to initialize mapper with %s", basicMapReduceContext), e);
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
      throw new IOException("Failed to flush operations at the end of mapper of " + basicMapReduceContext, e);
    }

    // Close all writers created by MultipleOutputs
    basicMapReduceContext.closeMultiOutputs();

    if (delegate instanceof ProgramLifecycle) {
      oldClassLoader = ClassLoaders.setContextClassLoader(weakReferenceClassLoader);
      try {
        ((ProgramLifecycle<? extends RuntimeContext>) delegate).destroy();
      } catch (Exception e) {
        LOG.error("Error during destroy of mapper {}", basicMapReduceContext, e);
        // Do nothing, try to finish
      } finally {
        ClassLoaders.setContextClassLoader(oldClassLoader);
      }
    }

    // Emit metrics one final time
    mapTaskMetricsWriter.reportMetrics();
  }

  private WrappedMapper.Context createAutoFlushingContext(final Context context,
                                                          final BasicMapReduceTaskContext basicMapReduceContext,
                                                          final MapTaskMetricsWriter metricsWriter) {
    // NOTE: we will change auto-flush to take into account size of buffered data, so no need to do/test a lot with
    //       current approach
    final int flushFreq = context.getConfiguration().getInt("c.mapper.flush.freq", 10000);
    final long reportIntervalInMillis = basicMapReduceContext.getMetricsReportIntervalMillis();

    @SuppressWarnings("unchecked")
    WrappedMapper.Context flushingContext = new WrappedMapper().new Context(context) {
      private int processedRecords;
      private long nextTimeToReportMetrics;

      @Override
      public boolean nextKeyValue() throws IOException, InterruptedException {
        boolean result = super.nextKeyValue();
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

      @Override
      public InputSplit getInputSplit() {
        InputSplit inputSplit = super.getInputSplit();
        if (inputSplit instanceof TaggedInputSplit) {
          // expose the delegate InputSplit to the user
          inputSplit = ((TaggedInputSplit) inputSplit).getInputSplit();
        }
        return inputSplit;
      }

      @Override
      public Class<? extends InputFormat<?, ?>> getInputFormatClass() throws ClassNotFoundException {
        InputSplit inputSplit = super.getInputSplit();
        if (inputSplit instanceof MultiInputTaggedSplit) {
          // expose the delegate InputFormat to the user
          return ((MultiInputTaggedSplit) inputSplit).getInputFormatClass();
        }
        return super.getInputFormatClass();
      }
    };
    return flushingContext;
  }

  private Mapper createMapperInstance(ClassLoader classLoader, String userMapper, Context context, String program) {
    if (context.getInputSplit() instanceof MultiInputTaggedSplit) {
      // Find the delegate Mapper from the MultiInputTaggedSplit.
      userMapper = ((MultiInputTaggedSplit) context.getInputSplit()).getMapperClassName();
    }
    try {
      return (Mapper) classLoader.loadClass(userMapper).newInstance();
    } catch (Exception e) {
      Throwable rootCause = Throwables.getRootCause(e);
      LOG.error("Failed to create instance of the user-defined Mapper class: " + userMapper);
      USERLOG.error("Failed to create mapper instance for program '{}' with error: {}. Please check the system logs " +
                      "for more details.", program, rootCause.getMessage(), rootCause);
      throw Throwables.propagate(e);
    }
  }
}
