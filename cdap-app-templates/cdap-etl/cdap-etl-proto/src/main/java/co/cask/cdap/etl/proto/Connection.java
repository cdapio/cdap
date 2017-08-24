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

package co.cask.cdap.etl.proto;

import co.cask.cdap.etl.proto.v2.ETLStage;

import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Represents a connection between two {@link ETLStage}
 */
public class Connection {
  private final String from;
  private final String to;
  private final String port;
  private final Boolean condition;

  public Connection(String from, String to) {
    this(from, to, null, null);
  }

  public Connection(String from, String to, @Nullable String port) {
    this(from, to, port, null);
  }

  public Connection(String from, String to, @Nullable Boolean condition) {
    this(from, to, null, condition);
  }

  private Connection(String from, String to, @Nullable String port, @Nullable Boolean condition) {
    this.from = from;
    this.to = to;
    this.port = port;
    this.condition = condition;
  }

  public String getFrom() {
    return from;
  }

  public String getTo() {
    return to;
  }

  @Nullable
  public String getPort() {
    return port;
  }

  @Nullable
  public Boolean getCondition() {
    return condition;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Connection that = (Connection) o;

    return Objects.equals(from, that.from) && Objects.equals(to, that.to) && Objects.equals(port, that.port)
      && Objects.equals(condition, that.condition);
  }

  @Override
  public int hashCode() {
    return Objects.hash(from, to, port, condition);
  }

  @Override
  public String toString() {
    return "Connection{" +
      "from='" + from + '\'' +
      ", to='" + to + '\'' +
      ", port='" + port + '\'' +
      ", condition='" + condition + '\'' +
      '}';
  }
}
