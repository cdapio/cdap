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
 *
 */

package io.cdap.cdap.etl.api.connector;

import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Connect request for a connector
 */
public class ConnectRequest {

  /**
   * The option that is possible for the request
   */
  public enum ConnectorOption {
    ALL,
    LIMIT,
    PROPERTIES
  }

  protected final String path;
  protected final Map<ConnectorOption, ?> options;

  public ConnectRequest(String path, Map<ConnectorOption, ?> options) {
    this.path = path;
    this.options = options;
  }

  public String getPath() {
    return path;
  }

  @Nullable
  @SuppressWarnings("unchecked")
  public <T> T getOption(ConnectorOption option) {
    return (T) options.get(option);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ConnectRequest that = (ConnectRequest) o;
    return Objects.equals(options, that.options) &&
      Objects.equals(path, that.path);
  }

  @Override
  public int hashCode() {
    return Objects.hash(path, options);
  }
}
