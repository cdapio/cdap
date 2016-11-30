/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.app.preview;

import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ProgramId;

/**
 * Represents the preview application request.
 * @param <T> the type of application config
 */
public class PreviewRequest<T> {
  private final ProgramId program;
  private final AppRequest<T> appRequest;

  public PreviewRequest(ProgramId program, AppRequest<T> appRequest) {
    this.program = program;
    this.appRequest = appRequest;
  }

  public ProgramId getProgram() {
    return program;
  }

  public AppRequest<T> getAppRequest() {
    return appRequest;
  }
}
