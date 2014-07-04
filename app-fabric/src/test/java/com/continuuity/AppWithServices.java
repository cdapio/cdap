package com.continuuity;

import com.continuuity.api.annotation.Handle;
import com.continuuity.api.app.AbstractApplication;
import com.continuuity.api.procedure.AbstractProcedure;
import com.continuuity.api.procedure.ProcedureRequest;
import com.continuuity.api.procedure.ProcedureResponder;
import org.apache.twill.api.AbstractTwillRunnable;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillSpecification;

import java.io.IOException;

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
    addProcedure(new NoOpProcedure());
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

  public static final class NoOpProcedure extends AbstractProcedure {
    @Handle("noop")
    public void handle(ProcedureRequest request,
                       ProcedureResponder responder)
      throws IOException {
      responder.sendJson("OK");
    }
  }
}
