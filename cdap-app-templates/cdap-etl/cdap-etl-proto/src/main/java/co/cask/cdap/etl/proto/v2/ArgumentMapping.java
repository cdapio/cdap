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

package co.cask.cdap.etl.proto.v2;

import javax.annotation.Nullable;

/**
 * The mapping between a triggering pipeline argument to a triggered pipeline argument.
 */
public class ArgumentMapping {
  @Nullable
  private final String source;
  @Nullable
  private final String target;

  public ArgumentMapping(@Nullable String source, @Nullable String target) {
    this.source = source;
    this.target = target;
  }

  /**
   * @return The name of triggering pipeline argument
   */
  @Nullable
  public String getSource() {
    return source;
  }

  /**
   * @return The name of triggered pipeline argument
   */
  @Nullable
  public String getTarget() {
    return target;
  }

  @Override
  public String toString() {
    return "ArgumentMapping{" +
      "source='" + getSource() + '\'' +
      ", target='" + getTarget() + '\'' +
      '}';
  }
}
