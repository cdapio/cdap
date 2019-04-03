/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.runtime.spi;


/**
 * Spark compat versions.
 */
public enum SparkCompat {
  SPARK1_2_10("spark1_2.10"),
  SPARK2_2_11("spark2_2.11");

  private final String compat;

  SparkCompat(String compat) {
    this.compat = compat;
  }

  public String getCompat() {
    return compat;
  }
}
