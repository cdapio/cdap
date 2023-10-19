/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.proto.operation;

import java.util.Objects;

/**
 * Representation of a cdap resource on which an operation is executed.
 */
public class OperationResource {
  private final String resourceUri;

  public OperationResource(String resourceUri) {
    this.resourceUri = resourceUri;
  }

  public String getResourceUri() {
    return resourceUri;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    OperationResource that = (OperationResource) o;

    return Objects.equals(this.resourceUri, that.resourceUri);
  }

  @Override
  public int hashCode() {
    return Objects.hash(resourceUri);
  }
}
