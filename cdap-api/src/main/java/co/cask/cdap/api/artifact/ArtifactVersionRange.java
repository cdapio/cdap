/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.api.artifact;

/**
 * Represents a version range of an artifact.
 */
public class ArtifactVersionRange {
  protected final ArtifactVersion lower;
  protected final ArtifactVersion upper;
  protected final boolean isLowerInclusive;
  protected final boolean isUpperInclusive;

  public ArtifactVersionRange(ArtifactVersion lower, boolean isLowerInclusive,
                              ArtifactVersion upper, boolean isUpperInclusive) {
    this.lower = lower;
    this.upper = upper;
    this.isLowerInclusive = isLowerInclusive;
    this.isUpperInclusive = isUpperInclusive;
  }

  /**
   * lower version of artifact range
   * @return {@link ArtifactVersion} lower version range
   */
  public ArtifactVersion getLower() {
    return lower;
  }

  /**
   * upper version of artifact range
   * @return {@link ArtifactVersion} upper version range
   */
  public ArtifactVersion getUpper() {
    return upper;
  }

  public boolean versionIsInRange(ArtifactVersion version) {
    int lowerCompare = version.compareTo(lower);
    boolean lowerSatisfied = isLowerInclusive ? lowerCompare >= 0 : lowerCompare > 0;
    int upperCompare = version.compareTo(upper);
    boolean upperSatisfied = isUpperInclusive ? upperCompare <= 0 : upperCompare < 0;
    return lowerSatisfied && upperSatisfied;
  }

  public String getVersionString() {
    if (isExactVersion()) {
      return lower.getVersion();
    } else {
      return (isLowerInclusive ? '[' : '(') +
        lower.getVersion() +
        ',' +
        upper.getVersion() +
        (isUpperInclusive ? ']' : ')');
    }
  }

  private boolean isExactVersion() {
    return isLowerInclusive && isUpperInclusive && upper.equals(lower);
  }

  public static ArtifactVersionRange parse(String artifactVersionStr) throws InvalidArtifactRangeException {
    if (!isVersionRange(artifactVersionStr)) {
      ArtifactVersion version = new ArtifactVersion(artifactVersionStr);
      if (version.getVersion() == null) {
        throw new InvalidArtifactRangeException(String.format("Could not parse '%s' as an artifact version.", version));
      }
      return new ArtifactVersionRange(version, true, version, true);
    }

    boolean isLowerInclusive = artifactVersionStr.charAt(0) == '[';

    // search for the comma separating versions
    int commaIndex = artifactVersionStr.indexOf(',', 1);
    if (commaIndex < 0) {
      throw new InvalidArtifactRangeException(
        String.format("Invalid version range %s. Could not find ',' separating lower and upper verions.",
                      artifactVersionStr));
    }
    String lowerStr = artifactVersionStr.substring(1, commaIndex).trim();
    ArtifactVersion lower = new ArtifactVersion(lowerStr);
    if (lower.getVersion() == null) {
      throw new InvalidArtifactRangeException(String.format(
        "Invalid version range %s. Lower version %s is invalid.", artifactVersionStr, lowerStr));
    }

    // search for the ']' or ')' marking the end of the upper version
    int versionEndIndex = indexOf(artifactVersionStr, ']', ')', commaIndex + 1);
    if (versionEndIndex < 0) {
      throw new InvalidArtifactRangeException(String.format(
        "Invalid version range %s. Could not find enclosing ']' or ')'.", artifactVersionStr));
    }
    // if it does not end in ']' or ')'
    if (versionEndIndex != artifactVersionStr.length() - 1) {
      throw new InvalidArtifactRangeException(String.format(
        "Invalid version range %s. There are extra characters after enclosing ']' or ')'.", artifactVersionStr));
    }
    String upperStr = artifactVersionStr.substring(commaIndex + 1, versionEndIndex).trim();
    ArtifactVersion upper = new ArtifactVersion(upperStr);
    if (upper.getVersion() == null) {
      throw new InvalidArtifactRangeException(String.format(
        "Invalid version range %s. Upper version %s is invalid.", artifactVersionStr, upperStr));
    }
    boolean isUpperInclusive = artifactVersionStr.charAt(versionEndIndex) == ']';

    // check that lower is not greater than upper
    int comp = lower.compareTo(upper);
    if (comp > 0) {
      throw new InvalidArtifactRangeException(String.format(
        "Invalid version range %s. Lower version %s is greater than upper version %s.",
        artifactVersionStr, lowerStr, upperStr));
    } else if (comp == 0 && isLowerInclusive && !isUpperInclusive) {
      // if lower and upper are equal, but lower is inclusive and upper is exclusive, this is also invalid
      throw new InvalidArtifactRangeException(String.format(
        "Invalid version range %s. Lower and upper versions %s are equal, " +
          "but lower is inclusive and upper is exclusive.",
        artifactVersionStr, lowerStr));
    }
    return new ArtifactVersionRange(lower, isLowerInclusive, upper, isUpperInclusive);
  }

  // like String's indexOf(char, int), except it looks for either one of 2 characters
  private static int indexOf(String str, char option1, char option2, int startIndex) {
    for (int i = startIndex; i < str.length(); i++) {
      char charAtIndex = str.charAt(i);
      if (charAtIndex == option1 || charAtIndex == option2) {
        return i;
      }
    }
    return -1;
  }

  private static boolean isVersionRange(String version) {
    return (version.startsWith("[") || version.startsWith("("));
  }
}
