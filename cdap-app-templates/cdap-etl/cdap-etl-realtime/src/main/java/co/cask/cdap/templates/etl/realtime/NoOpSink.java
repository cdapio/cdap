package co.cask.cdap.templates.etl.realtime;

import co.cask.cdap.templates.etl.api.realtime.RealtimeConfigurer;
import co.cask.cdap.templates.etl.api.realtime.RealtimeSink;

/**
 *
 */
public class NoOpSink<T> extends RealtimeSink<T> {

  @Override
  public void configure(RealtimeConfigurer configurer) {
    configurer.setName(NoOpSink.class.getSimpleName());
  }

  @Override
  public void write(T object) {
    // no-op
  }
}
