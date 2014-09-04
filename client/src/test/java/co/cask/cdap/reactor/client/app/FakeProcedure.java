/*
 * Copyright 2014 Cask Data, Inc.
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

package co.cask.cdap.reactor.client.app;

import co.cask.cdap.api.annotation.Handle;
import co.cask.cdap.api.procedure.AbstractProcedure;
import co.cask.cdap.api.procedure.ProcedureRequest;
import co.cask.cdap.api.procedure.ProcedureResponder;
import com.google.common.collect.ImmutableMap;

import java.io.IOException;

/**
 *
 */
public class FakeProcedure extends AbstractProcedure {

  public static final String NAME = "FakeProcedure";
  public static final String METHOD_NAME = "fakeMethod";

  @Handle(METHOD_NAME)
  public void handle(ProcedureRequest request, ProcedureResponder responder) throws IOException {
    String word = request.getArgument("customer");
    responder.sendJson(ImmutableMap.of("customer", "real" + word));
  }
}
