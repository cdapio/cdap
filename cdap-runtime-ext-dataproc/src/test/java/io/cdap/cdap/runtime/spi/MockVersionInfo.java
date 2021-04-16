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

package io.cdap.cdap.runtime.spi;

/**
 * A mock version info. Does not do a proper numeric compare, so please use it for versions with same number of
 * decimal digits.
 */
public class MockVersionInfo implements VersionInfo {
  private final String version;

  public MockVersionInfo(String version) {
    this.version = version;
  }

  @Override
  public int compareTo(Object o) {
    return version.compareTo(o.toString());
  }

  @Override
  public String toString() {
    return version;
  }
}
