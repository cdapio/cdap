package com.continuuity.internal.app.runtime.batch;

import com.continuuity.api.data.OperationException;
import com.continuuity.common.logging.LoggingContextAccessor;
import com.google.common.base.Throwables;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Wraps user-defined implementation of {@link Reducer} class which allows perform extra configuration.
 */
public class ReducerWrapper extends Reducer {

  public static final String ATTR_REDUCER_CLASS = "c.reducer.class";

  private static final Logger LOG = LoggerFactory.getLogger(MapperWrapper.class);

  @Override
  public void run(Context context) throws IOException, InterruptedException {
    MapReduceContextProvider mrContextProvider = new MapReduceContextProvider(context);
    BasicMapReduceContext basicMapReduceContext = mrContextProvider.get();
    try {
      String userReducer = context.getConfiguration().get(ATTR_REDUCER_CLASS);
      Reducer delegate = createReducerInstance(context.getConfiguration().getClassLoader(), userReducer);

      // injecting runtime components, like datasets, etc.
      basicMapReduceContext.injectFields(delegate);

      LoggingContextAccessor.setLoggingContext(basicMapReduceContext.getLoggingContext());

      delegate.run(context);

      // transaction is not finished, but we want all operations to be dispatched (some could be buffered in
      // memory by tx agent
      try {
        basicMapReduceContext.flushOperations();
      } catch (OperationException e) {
        LOG.error("Failed to flush operations at the end of reducer of " + basicMapReduceContext.toString());
        throw Throwables.propagate(e);
      }
    } finally {
      basicMapReduceContext.close(); // closes all datasets
    }
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
