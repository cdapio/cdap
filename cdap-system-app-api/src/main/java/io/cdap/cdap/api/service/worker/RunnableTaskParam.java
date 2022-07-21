/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.api.service.worker;

import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Class for the parameter of {@link RunnableTaskRequest}
 */
public class RunnableTaskParam {

  /**
   * Param string value
   */
  private final String simpleParam;
  /**
   * Param class name
   */
  private final RunnableTaskRequest embeddedTaskRequest;

  public RunnableTaskParam(@Nullable String simpleParam, @Nullable RunnableTaskRequest embeddedTaskRequest) {
    this.simpleParam = simpleParam;
    this.embeddedTaskRequest = embeddedTaskRequest;
  }

  @Nullable
  public RunnableTaskRequest getEmbeddedTaskRequest() {
    return embeddedTaskRequest;
  }

  @Nullable
  public String getSimpleParam() {
    return simpleParam;
  }

  @Override
  public String toString() {
    String format = "RunnableTaskParam{simpleParam='%s' , embeddedTaskRequest='%s'}";
    return String.format(format, simpleParam, embeddedTaskRequest);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RunnableTaskParam)) {
      return false;
    }
    RunnableTaskParam that = (RunnableTaskParam) o;
    return Objects.equals(simpleParam, that.simpleParam) && Objects
      .equals(embeddedTaskRequest, that.embeddedTaskRequest);
  }

  @Override
  public int hashCode() {
    return Objects.hash(simpleParam, embeddedTaskRequest);
  }
}
