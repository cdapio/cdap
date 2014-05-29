package com.continuuity;

import com.continuuity.api.AbstractApplication;
import com.continuuity.api.ApplicationConfigurer;
import com.continuuity.api.ApplicationContext;
import org.apache.twill.api.AbstractTwillRunnable;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillSpecification;

/**
 *
 */
public class AppWithServices extends AbstractApplication {

  @Override
  public void configure(ApplicationConfigurer configurer, ApplicationContext context) {
    configurer.setName("AppWithServices");
    configurer.setDescription("Application with services");
    configurer.addService(new TwillApplication() {
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
    });
  }

  public static final class DummyService extends AbstractTwillRunnable {
    @Override
    public void run() {
     //No-op
    }
  }
}
