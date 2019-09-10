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

// FE implementation of ArtifactVersion java class
// co.cask.cdap.api.artifact.ArtifactVersion
//
// [major].[minor].[fix](-)[suffix]

export default class Version {
  constructor(versionString) {
    let major,
        minor,
        fix,
        suffix;

    // Get Suffix
    let versionSuffixSplit = versionString.split('-');
    suffix = versionSuffixSplit[1] || null;

    // Split versions
    let versionSplit = versionSuffixSplit[0].split('.');
    major = versionSplit[0];
    minor = versionSplit[1];
    fix = versionSplit[2];

    this.version = versionString;
    this.major = parseInt(major, 10) || null;
    this.minor = parseInt(minor, 10) || null;
    this.fix = parseInt(fix, 10) || null;
    this.suffix = suffix;
  }

  isSnapshot() {
    return this.suffix !== null && this.suffix.length !== 0 && this.suffix.toLowerCase() === 'snapshot';
  }

  // -1, 0, 1   smaller, equal, greater
  // special case for no suffix is greater than with suffix. (release > snapshot)
  compareTo(other) {
    let cmp = this.compare(this.major, other.major);
    if (cmp !== 0) {
      return cmp;
    }

    cmp = this.compare(this.minor, other.minor);
    if (cmp !== 0) {
      return cmp;
    }

    cmp = this.compare(this.fix, other.fix);
    if (cmp !== 0) {
      return cmp;
    }

    // All numerical part of the version are the same, compare the suffix.
    // A special case is no suffix is "greater" than with suffix. This is usually true (e.g. release > snapshot)
    if (this.suffix === null) {
      return other.suffix === null ? 0 : 1;
    } else if (other.suffix === null) {
      return -1;
    } else if (this.suffix !== null && other.suffix !== null) {
      return this.compareVersion(this.suffix, other.suffix);
    }
  }
  compareVersion(v1, v2) {
    if (typeof v1 !== 'string') {
      return false;
    }
    if (typeof v2 !== 'string') {
      return false;
    }
    v1 = v1.split('.');
    v2 = v2.split('.');
    const k = Math.min(v1.length, v2.length);
    for (let i = 0; i < k; ++ i) {
        v1[i] = parseInt(v1[i], 10);
        v2[i] = parseInt(v2[i], 10);
        if (v1[i] > v2[i]) {
          return 1;
        }
        if (v1[i] < v2[i]) {
          return -1;
        }
    }
    return v1.length == v2.length ? 0: (v1.length < v2.length ? -1 : 1);
  }
  compare(first, second) {
    if ((first === null && second === null) || first === second) {
      return 0;
    }

    if (first === null || first < second) {
      return -1;
    }

    if (second === null || first > second) {
      return 1;
    }

    // for invalid comparison
    return null;
  }

  toString() {
    return this.version;
  }
}
