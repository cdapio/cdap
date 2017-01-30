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

import Version from 'services/VersionRange/Version';

// FE implementation of ArtifactRange
// co.cask.cdap.proto.artifact.ArtifactRange
export default class VersionRange {
  constructor(range) {
    let {
      lower,
      upper,
      isLowerInclusive,
      isUpperInclusive
    } = parseRange(range);

    this.lower = lower;
    this.upper = upper;
    this.isLowerInclusive = isLowerInclusive;
    this.isUpperInclusive = isUpperInclusive;
  }

  versionIsInRange(version) {
    let lowerCompare = version.compareTo(this.lower);
    let lowerSatisfied = this.isLowerInclusive ? lowerCompare >= 0 : lowerCompare > 0;

    let upperCompare = version.compareTo(this.upper);
    let upperSatisfied = this.isUpperInclusive ? upperCompare <= 0 : upperCompare < 0;

    return lowerSatisfied && upperSatisfied;
  }
}

function parseRange(range) {
  let lower,
      upper,
      isLowerInclusive,
      isUpperInclusive;

  let trimmedRange = range.trim();

  isLowerInclusive = trimmedRange.charAt(0) === '[';
  isUpperInclusive = trimmedRange.charAt(trimmedRange.length - 1) === ']';

  let split = trimmedRange.split(',');

  lower = new Version(split[0].trim().substr(1).trim());
  upper = new Version(split[1].trim().substr(0, split[1].length - 1).trim());

  return {
    lower,
    upper,
    isLowerInclusive,
    isUpperInclusive
  };
}
