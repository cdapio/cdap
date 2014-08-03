/*
 * Copyright 2014 Cask, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.gateway.handlers;

import co.cask.cdap.api.annotation.Handle;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.procedure.AbstractProcedure;
import co.cask.cdap.api.procedure.ProcedureRequest;
import co.cask.cdap.api.procedure.ProcedureResponder;
import co.cask.cdap.api.procedure.ProcedureSpecification;

/**
 * App to test Procedure API Handling.
 */
public class ProcedureTestApp extends AbstractApplication {
  @Override
  public void configure() {
    setName("ProcedureTestApp");
    setDescription("App to test Procedure API Handling");
    addProcedure(new TestProcedure());
  }

  /**
   * TestProcedure handler.
   */
  public static class TestProcedure extends AbstractProcedure {
    @Override
    public ProcedureSpecification configure() {
      return ProcedureSpecification.Builder.with()
        .setName("TestProcedure")
        .setDescription("Test Procedure")
        .build();
    }

    @SuppressWarnings("UnusedDeclaration")
    @Handle("TestMethod")
    public void testMethod1(ProcedureRequest request, ProcedureResponder responder) throws Exception {
      responder.sendJson(request.getArguments());
    }
  }
}
