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

package io.cdap.cdap.spi.events;

import io.cdap.cdap.spi.events.trigger.TriggeringInfo;

import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Represents how a run has been triggered and trigger details
 */
public class StartMetadata {

  private final StartType type;
  @Nullable
  private final TriggeringInfo triggeringInfo;

  public StartMetadata(StartType type, TriggeringInfo triggeringInfo) {
    this.type = type;
    this.triggeringInfo = triggeringInfo;
  }

  /**
   * @return The type of {@link StartMetadata}
   */
  public StartType getType() {
    return type;
  }

  /**
   * @return The details of the trigger
   */
  @Nullable
  public TriggeringInfo getTriggeringInfo() {
    return triggeringInfo;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof StartMetadata)) {
      return false;
    }
    StartMetadata that = (StartMetadata) o;
    return getType() == that.getType()
      && Objects.equals(getTriggeringInfo(), that.getTriggeringInfo());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getType(), getTriggeringInfo());
  }
}
