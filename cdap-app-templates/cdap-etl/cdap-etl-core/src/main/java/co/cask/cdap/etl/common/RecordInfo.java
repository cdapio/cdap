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

package co.cask.cdap.etl.common;

import java.io.Serializable;
import javax.annotation.Nullable;

/**
 * Contains the record value output by a stage as well as context information about the record, including the stage
 * it was emitted from and the port it was emitted from.
 *
 * @param <T> the type of value
 */
public class RecordInfo<T> implements Serializable {
  private static final long serialVersionUID = 2507536440619795611L;
  private final T value;
  private final String fromStage;
  private final String fromPort;
  private final boolean isError;

  private RecordInfo(T value, String fromStage, String fromPort, boolean isError) {
    this.fromStage = fromStage;
    this.value = value;
    this.fromPort = fromPort;
    this.isError = isError;
  }

  public T getValue() {
    return value;
  }

  public String getFromStage() {
    return fromStage;
  }

  /**
   * @return the port the record was emitted from, or null if it was not emitted from a port.
   */
  @Nullable
  public String getFromPort() {
    return fromPort;
  }

  public boolean isError() {
    return isError;
  }

  /**
   * @return builder for an RecordInfo
   */
  public static <T> Builder<T> builder(T value, String fromStage) {
    return new Builder<>(value, fromStage);
  }

  /**
   * Builder for an RecordInfo
   *
   * @param <T> type of record value
   */
  public static class Builder<T> {
    private final T value;
    private final String fromStage;
    private String fromPort;
    private boolean isError;

    private Builder(T value, String fromStage) {
      this.value = value;
      this.fromStage = fromStage;
      this.isError = false;
    }

    public Builder<T> fromPort(String port) {
      this.fromPort = port;
      return this;
    }

    public Builder<T> isError() {
      this.isError = true;
      return this;
    }

    public RecordInfo<T> build() {
      return new RecordInfo<>(value, fromStage, fromPort, isError);
    }
  }
}
