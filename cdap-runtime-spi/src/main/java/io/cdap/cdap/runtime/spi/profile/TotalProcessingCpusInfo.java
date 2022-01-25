/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.runtime.spi.profile;

public class TotalProcessingCpusInfo {

  public enum PrefixLabel {
    FIXED (""),
    CUSTOM ("Custom"),
    UP_TO ("Up to");

    private String value;

    PrefixLabel(String value) {
      this.value = value;
    }

    public String getValue() {
      return value;
    }
  }

  private final int maxCpus;
  private final PrefixLabel prefixLabel;

  public TotalProcessingCpusInfo(int maxCpus, PrefixLabel prefixLabel) {
    this.maxCpus = maxCpus;
    this.prefixLabel = prefixLabel;
  }

  public static TotalProcessingCpusInfo getDefault() {
    return new TotalProcessingCpusInfo(-1, PrefixLabel.CUSTOM);
  }

  public String getFullLabel() {
    StringBuilder sb = new StringBuilder();

    if (prefixLabel != null) {
      sb.append(prefixLabel.getValue()).append(" ");
    }

    if (maxCpus > 0) {
      sb.append(maxCpus);
    }
    return sb.toString().trim();
  }
}
