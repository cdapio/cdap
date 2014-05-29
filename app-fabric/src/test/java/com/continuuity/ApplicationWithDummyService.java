package com.continuuity;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.data.stream.Stream;
import org.apache.twill.api.AbstractTwillRunnable;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillSpecification;

/**
 * Application with a dummy service with existing Application API.
 */
public class ApplicationWithDummyService implements Application {

  /**
   * Configures the {@link com.continuuity.api.Application} by returning an {@link ApplicationSpecification}.
   *
   * @return An instance of {@link com.continuuity.api.ApplicationSpecification}.
   */
  @Override
  public ApplicationSpecification configure() {
    return ApplicationSpecification.Builder.with()
             .setName("ResourceApp")
             .setDescription("Resource Application")
             .withStreams().add(new Stream("X"))
             .noDataSet()
             .noFlow()
             .noProcedure()
             .noMapReduce()
             .noWorkflow()
             .withServices().add(new DummyTwillApplication())
             .build();
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
