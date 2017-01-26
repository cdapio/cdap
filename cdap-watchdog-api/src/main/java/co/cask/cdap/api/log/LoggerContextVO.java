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
package co.cask.cdap.api.log;

import java.util.Map;

/**
 * Replica of Logback LoggerContextVO
 */
public class LoggerContextVO {
  private final String name;
  private final Map<String, String> propertyMap;
  private final long birthTime;

  public LoggerContextVO(String name, Map<String, String> propertyMap, long birthTime) {
    this.name = name;
    this.propertyMap = propertyMap;
    this.birthTime = birthTime;
  }

  public String getName() {
    return name;
  }

  public Map<String, String> getPropertyMap() {
    return propertyMap;
  }

  public long getBirthTime() {
    return birthTime;
  }

}
