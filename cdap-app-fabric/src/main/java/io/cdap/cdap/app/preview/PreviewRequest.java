/*
 * Copyright © 2016-2020 Cask Data, Inc.
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

package io.cdap.cdap.app.preview;

import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.artifact.preview.PreviewConfig;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ProgramId;

/**
 * Represents the preview application request.
 */
public class PreviewRequest {
  private final ProgramId program;
  private final AppRequest<?> appRequest;

  public PreviewRequest(ProgramId program, AppRequest<?> appRequest) {
    this.program = program;
    this.appRequest = appRequest;
  }

  public PreviewRequest(ApplicationId applicationId, AppRequest<?> appRequest) {
    this(getProgramIdFromRequest(applicationId, appRequest), appRequest);
  }

  public ProgramId getProgram() {
    return program;
  }

  public AppRequest<?> getAppRequest() {
    return appRequest;
  }

  private static ProgramId getProgramIdFromRequest(ApplicationId preview, AppRequest request) {
    PreviewConfig previewConfig = request.getPreview();
    if (previewConfig == null) {
      throw new IllegalArgumentException("Preview config cannot be null");
    }

    String programName = previewConfig.getProgramName();
    ProgramType programType = previewConfig.getProgramType();

    if (programName == null || programType == null) {
      throw new IllegalArgumentException("ProgramName or ProgramType cannot be null.");
    }

    return preview.program(programType, programName);
  }
}
