/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.gateway.handlers;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.annotation.Handle;
import com.continuuity.api.procedure.AbstractProcedure;
import com.continuuity.api.procedure.ProcedureRequest;
import com.continuuity.api.procedure.ProcedureResponder;
import com.continuuity.api.procedure.ProcedureSpecification;

/**
 * App to test Procedure API Handling.
 */
public class ProcedureTestApp implements Application {
  @Override
  public ApplicationSpecification configure() {
    return ApplicationSpecification.Builder.with()
      .setName("ProcedureTestApp")
      .setDescription("App to test Procedure API Handling")
      .noStream()
      .noDataSet()
      .noFlow()
      .withProcedures()
      .add(new TestProcedure())
      .noMapReduce()
      .noWorkflow()
      .build();
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
