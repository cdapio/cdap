/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.monitor;

import io.cdap.cdap.proto.id.ProgramRunId;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Result of validating an incoming runtime request, carrying the run status along with the
 * authenticated run identity and parsed {@link RuntimeScopes} when a {@code RUNTIME} token was
 * presented.
 */
public final class ValidatedRuntimeRequest {

  private final ProgramRunInfo programRunInfo;
  @Nullable
  private final ProgramRunId tokenRunId;
  @Nullable
  private final RuntimeScopes runtimeScopes;

  /**
   * Creates a validation result for a legacy (non-{@code RUNTIME}) request.
   */
  public static ValidatedRuntimeRequest legacy(ProgramRunInfo programRunInfo) {
    return new ValidatedRuntimeRequest(programRunInfo, null, null);
  }

  /**
   * Creates a validation result for a request authenticated with a {@code RUNTIME} token.
   */
  public static ValidatedRuntimeRequest runtimeToken(ProgramRunInfo programRunInfo,
      ProgramRunId tokenRunId, @Nullable RuntimeScopes runtimeScopes) {
    return new ValidatedRuntimeRequest(programRunInfo,
        Objects.requireNonNull(tokenRunId, "tokenRunId"), runtimeScopes);
  }

  private ValidatedRuntimeRequest(ProgramRunInfo programRunInfo, @Nullable ProgramRunId tokenRunId,
      @Nullable RuntimeScopes runtimeScopes) {
    this.programRunInfo = Objects.requireNonNull(programRunInfo, "programRunInfo");
    this.tokenRunId = tokenRunId;
    this.runtimeScopes = runtimeScopes;
  }

  public ProgramRunInfo getProgramRunInfo() {
    return programRunInfo;
  }

  public boolean isRuntimeToken() {
    return tokenRunId != null;
  }

  @Nullable
  public ProgramRunId getTokenRunId() {
    return tokenRunId;
  }

  @Nullable
  public RuntimeScopes getRuntimeScopes() {
    return runtimeScopes;
  }
}
