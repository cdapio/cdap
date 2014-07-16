/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.gateway.handlers.log;

/**
* Test Log object.
*/
class LogLine {
  private final long offset;
  private final String log;

  LogLine(long offset, String log) {
    this.offset = offset;
    this.log = log;
  }

  public long getOffset() {
    return offset;
  }

  public String getLog() {
    return log;
  }

  @Override
  public String toString() {
    return "LogLine{" +
      "offset=" + offset +
      ", log='" + log + '\'' +
      '}';
  }
}
