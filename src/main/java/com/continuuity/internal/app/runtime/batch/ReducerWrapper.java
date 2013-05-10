package com.continuuity.internal.app.runtime.batch;

import com.continuuity.common.logging.LoggingContextAccessor;
import com.google.common.base.Throwables;
import org.apache.hadoop.mapreduce.Mapper;
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
    String userReducer = context.getConfiguration().get(ATTR_REDUCER_CLASS);
    Reducer delegate = createReducerInstance(userReducer);

    MapReduceContextProvider mrContextProvider = new MapReduceContextProvider(context);
    BasicMapReduceContext basicMapReduceContext = mrContextProvider.get();

    // injecting runtime components, like datasets, etc.
    basicMapReduceContext.injectFields(delegate);

    LoggingContextAccessor.setLoggingContext(basicMapReduceContext.getLoggingContext());

    delegate.run(context);
  }

  private Reducer createReducerInstance(String userReducer) {
    try {
      return (Reducer) Class.forName(userReducer).newInstance();
    } catch (Exception e) {
      LOG.error("Failed to create instance of the user-defined Reducer class: " + userReducer);
      throw Throwables.propagate(e);
    }
  }
}
