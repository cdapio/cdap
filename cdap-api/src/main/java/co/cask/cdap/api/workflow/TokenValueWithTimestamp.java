/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.api.workflow;

/**
 * Class representing the value for the key along with the timestamp inside the {@link WorkflowToken}.
 */
public final class TokenValueWithTimestamp {
  private final String value;
  private final long timeStamp;

  public TokenValueWithTimestamp(String value, long timeStamp) {
    this.value = value;
    this.timeStamp = timeStamp;
  }

  public String getValue() {
    return value;
  }

  public long getTimeStamp() {
    return timeStamp;
  }
}

