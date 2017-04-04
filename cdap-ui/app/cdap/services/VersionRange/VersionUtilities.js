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

export function findHighestVersion(versions, returnString) {
  let highestVersion;

  versions.forEach((version) => {
    let currentVersion = version;
    if (typeof currentVersion === 'string') {
      currentVersion = new Version(currentVersion);
    }

    if (!highestVersion || currentVersion.compareTo(highestVersion) === 1) {
      highestVersion = currentVersion;
    }
  });

  return returnString ? highestVersion.toString() : highestVersion;
}

export function findLowestVersion(versions, returnString) {
  let lowestVersion;

  versions.forEach((version) => {
    let currentVersion = version;
    if (typeof currentVersion === 'string') {
      currentVersion = new Version(currentVersion);
    }

    if (!lowestVersion || currentVersion.compareTo(lowestVersion) === -1) {
      lowestVersion = currentVersion;
    }
  });

  return returnString ? lowestVersion.toString() : lowestVersion;
}
