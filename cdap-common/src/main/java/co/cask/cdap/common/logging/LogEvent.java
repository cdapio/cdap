/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.common.logging;

/**
 * Represents a logging event.
 */
public class LogEvent {

  public static final String FIELD_NAME_LOGTAG = "logtag";
  public static final String FIELD_NAME_LOGLEVEL = "level";

  private final String tag;
  private final String level;
  private final String message;

  public LogEvent(String logtag, String level, String message) {
    this.tag = logtag;
    this.level = level;
    this.message = message;
  }

  public String getTag() {
    return tag;
  }

  public String getLevel() {
    return level;
  }

  public String getMessage() {
    return message;
  }

}
