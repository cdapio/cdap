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

/**
 * Replica of Logback ClassPackagingData
 */
public final class ClassPackagingData {
  private final String codeLocation;
  private final String version;
  private final boolean exact;

  public ClassPackagingData(String codeLocation, String version, boolean exact) {
    this.codeLocation = codeLocation;
    this.version = version;
    this.exact = exact;
  }
  public String getCodeLocation() {
    return codeLocation;
  }
  public String getVersion() {
    return version;
  }
  public boolean isExact() {
    return exact;
  }
}
