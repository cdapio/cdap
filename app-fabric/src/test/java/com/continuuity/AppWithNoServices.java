package com.continuuity;

import com.continuuity.api.annotation.Handle;
import com.continuuity.api.app.AbstractApplication;
import com.continuuity.api.procedure.AbstractProcedure;
import com.continuuity.api.procedure.ProcedureRequest;
import com.continuuity.api.procedure.ProcedureResponder;

import java.io.IOException;

/**
 *
 */
public class AppWithNoServices extends AbstractApplication {
  /**
   * Override this method to configure the application.
   */
  @Override
  public void configure() {
    setName("AppWithServices");
    setDescription("Application with Services");
    addProcedure(new NoOpProcedure());
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
