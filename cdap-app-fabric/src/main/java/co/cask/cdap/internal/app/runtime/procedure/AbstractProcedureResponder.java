/*
 * Copyright © 2014 Cask Data, Inc.
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
package co.cask.cdap.internal.app.runtime.procedure;

import co.cask.cdap.api.procedure.ProcedureResponder;
import co.cask.cdap.api.procedure.ProcedureResponse;

import java.io.IOException;

/**
 * An abstract base class for implementing different {@link ProcedureResponder}.
 */
public abstract class AbstractProcedureResponder implements ProcedureResponder {

  @Override
  public final void sendJson(Object object) throws IOException {
    sendJson(ProcedureResponse.Code.SUCCESS, object);
  }

  @Override
  public final void sendJson(ProcedureResponse.Code code, Object object) throws IOException {
    sendJson(new ProcedureResponse(code), object);
  }
}
