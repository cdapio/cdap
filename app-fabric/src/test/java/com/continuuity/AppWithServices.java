package com.continuuity;

import com.continuuity.api.app.AbstractApplication;
import org.apache.twill.api.AbstractTwillRunnable;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillSpecification;

/**
 * Test Application with services for the new application API.
 */
public class AppWithServices extends AbstractApplication {
  /**
   * Override this method to configure the application.
   */
  @Override
  public void configure() {
    setName("AppWithServices");
    setDescription("Application with Services");
    addService(new DummyTwillApplication());
  }

  public static final class DummyTwillApplication implements TwillApplication {
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
    }
  }
}
