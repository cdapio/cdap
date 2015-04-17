package co.cask.cdap.templates.etl.realtime;

import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.templates.etl.api.ValueEmitter;
import co.cask.cdap.templates.etl.api.realtime.RealtimeConfigurer;
import co.cask.cdap.templates.etl.api.realtime.RealtimeSource;
import co.cask.cdap.templates.etl.api.realtime.SourceState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 *
 */
public class HelloSource extends RealtimeSource<String> {
  private static final Logger LOG = LoggerFactory.getLogger(HelloSource.class);
  private Metrics metrics;

  @Override
  public void configure(RealtimeConfigurer configurer) {
    configurer.setName(HelloSource.class.getSimpleName());
  }

  @Nullable
  @Override
  public SourceState poll(ValueEmitter<String> writer, SourceState currentState) {
    try {
      TimeUnit.SECONDS.sleep(1);
    } catch (InterruptedException e) {
      LOG.error("Some Error in Source");
    }
    LOG.info("Emitting data! Yippie!");
    metrics.count("Hello", 1);
    writer.emit("Hello");
    return null;
  }
}
