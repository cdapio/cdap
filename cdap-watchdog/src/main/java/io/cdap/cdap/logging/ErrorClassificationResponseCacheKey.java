/*
 * Copyright © 2025 Cask Data, Inc.
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

package io.cdap.cdap.logging;

import java.util.Objects;

/**
 * Cache key for {@link io.cdap.cdap.proto.ErrorClassificationResponse}.
 */
public final class ErrorClassificationResponseCacheKey {
  private final String namespace;
  private final String program;
  private final String appId;
  private final String runId;

  /**
   * Constructor for {@link ErrorClassificationResponseCacheKey}.
   */
  public ErrorClassificationResponseCacheKey(String namespace, String program,
      String appId, String runId) {
    this.namespace = namespace;
    this.program = program;
    this.appId = appId;
    this.runId = runId;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof ErrorClassificationResponseCacheKey)) {
      return false;
    }
    ErrorClassificationResponseCacheKey that = (ErrorClassificationResponseCacheKey) o;
    return Objects.equals(this.namespace, that.namespace)
        && Objects.equals(this.program, that.program) && Objects.equals(this.appId, that.appId)
        && Objects.equals(this.runId, that.runId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(namespace, program, appId, runId);
  }
}
