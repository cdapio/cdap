package com.continuuity.test.app;

import com.continuuity.api.app.AbstractApplication;
import org.apache.twill.api.AbstractTwillRunnable;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillSpecification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * App with just a single service.
 */
public class AppWithOnlyService extends AbstractApplication {
  private static final Logger LOG = LoggerFactory.getLogger(AppWithOnlyService.class);

  @Override
  public void configure() {
    setName("ServiceOnlyApp");
    addService(new TwillService());
  }

  public static class TwillService implements TwillApplication {
    @Override
    public TwillSpecification configure() {
      return TwillSpecification.Builder.with()
        .setName("NoOpService")
        .withRunnable()
        .add(new DummyService())
        .noLocalFiles()
        .anyOrder()
        .build();
    }
  }

  public static final class DummyService extends AbstractTwillRunnable {
    @Override
    public void run() {
      //No-op
      LOG.info("Runnable DummyService Started");
    }
    @Override
    public void stop() {
      LOG.info("Runnable DummyService Stopped");
    }
  }
}
