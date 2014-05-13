/**
 * Copyright 2013-2014 Continuuity, Inc.
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
package com.continuuity.examples.resourcespammer;

import com.continuuity.api.Resources;
import com.continuuity.api.annotation.Handle;
import com.continuuity.api.procedure.AbstractProcedure;
import com.continuuity.api.procedure.ProcedureRequest;
import com.continuuity.api.procedure.ProcedureResponder;
import com.continuuity.api.procedure.ProcedureResponse;
import com.continuuity.api.procedure.ProcedureSpecification;

/**
 * Procedure designed to use lots of CPU resources{@code Spammer1Core}.
 */
public class CPUSpamProcedure extends AbstractProcedure {
  private final Spammer spammer;

  @Override
  public ProcedureSpecification configure() {
    return ProcedureSpecification.Builder.with()
      .setName("spamProcedure")
      .setDescription("procedure that spams cpu")
      .withResources(new Resources(512, 2))
      .build();
  }

  public CPUSpamProcedure() {
    spammer = new Spammer(32);
  }

  @Handle("spam")
  public void spam(ProcedureRequest request, ProcedureResponder responder) throws Exception {
    long start = System.currentTimeMillis();
    spammer.spam();
    long duration = System.currentTimeMillis() - start;
    responder.sendJson(ProcedureResponse.Code.SUCCESS, duration);
  }
}
