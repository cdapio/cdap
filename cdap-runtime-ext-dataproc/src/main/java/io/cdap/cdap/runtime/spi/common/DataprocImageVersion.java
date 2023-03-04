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

package io.cdap.cdap.runtime.spi.common;

import com.google.common.base.Strings;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Class used to compare versions from Dataproc cluster images
 */
public class DataprocImageVersion implements Comparable<Object> {

  private static final Pattern IS_NUMBER_PATTERN = Pattern.compile("^\\d+$");
  private final List<Integer> versionSegments;

  public DataprocImageVersion(@Nullable String imageVersion) {
    // For null versions, we assume zero.
    if (Strings.isNullOrEmpty(imageVersion)) {
      throw new IllegalArgumentException("Invalid image version: " + imageVersion);
    }

    versionSegments = new ArrayList<>();

    // If the version contains a hyphen, only take the first portion (e.g. 1.2.3-debian4)
    if (imageVersion.contains("-")) {
      imageVersion = imageVersion.split("-")[0];
    }

    for (String component : imageVersion.split("\\.")) {
      // Check if each portion of the version is not empty and contains only numbers
      if (!IS_NUMBER_PATTERN.matcher(component).matches()) {
        throw new IllegalArgumentException("Invalid image version: " + imageVersion);
      }

      Integer part = Strings.isNullOrEmpty(component) ? 0 : Integer.parseInt(component);
      versionSegments.add(part);
    }
  }

  @Override
  public int compareTo(Object o) {
    DataprocImageVersion other;
    if (o == null) {
      return 1;
    } else if (o instanceof DataprocImageVersion) {
      other = (DataprocImageVersion) o;
    } else {
      other = new DataprocImageVersion(o.toString());
    }

    // Go over each element of the version and compare.
    // if elements have different lengths, 0 is taken as the value for each of the missing segments
    for (int i = 0; i < Math.max(this.versionSegments.size(), other.versionSegments.size()); i++) {
      int thisSegment = this.versionSegments.size() > i ? this.versionSegments.get(i) : 0;
      int otherSegment = other.versionSegments.size() > i ? other.versionSegments.get(i) : 0;

      if (thisSegment < otherSegment) {
        return -1;
      } else if (thisSegment > otherSegment) {
        return 1;
      }
    }

    return 0;
  }

  @Override
  public String toString() {
    String output = this.versionSegments.stream().map(Object::toString)
        .collect(Collectors.joining("."));

    if (output.isEmpty()) {
      return "0";
    }

    return output;
  }
}
