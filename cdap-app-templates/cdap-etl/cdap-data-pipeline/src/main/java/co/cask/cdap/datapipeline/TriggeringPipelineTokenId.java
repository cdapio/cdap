/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.datapipeline;

import java.util.Objects;

/**
 * Identifier of token from the triggering pipeline.
 */
public class TriggeringPipelineTokenId extends TriggeringPipelinePropertyId {
  private final String tokenKey;
  private final String nodeName;

  public TriggeringPipelineTokenId(String namespace, String pipelineName, String tokenKey, String nodeName) {
    super(Type.TOKEN, namespace, pipelineName);
    this.tokenKey = tokenKey;
    this.nodeName = nodeName;
  }

  /**
   * @return The key of the token in the triggering pipeline.
   */
  public String getTokenKey() {
    return tokenKey;
  }

  /**
   * @return The name of the node where the token is generated in the triggering pipeline.
   */
  public String getNodeName() {
    return nodeName;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    TriggeringPipelineTokenId that = (TriggeringPipelineTokenId) o;
    return Objects.equals(getTokenKey(), that.getTokenKey()) &&
      Objects.equals(getNodeName(), that.getNodeName());
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), getTokenKey(), getNodeName());
  }
}
