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

import VersionRange from 'services/VersionRange';
import Version from 'services/VersionRange/Version';
jest.disableAutomock();

describe('Version Range Class', () => {
  const EXCLUSIVE_RANGE = new VersionRange('[1.0.0, 1.1.0)');
  const INCLUSIVE_RANGE = new VersionRange('[1.0.0, 1.1.0]');
  const EXCLUSIVE_SNAPSHOT_RANGE = new VersionRange('[1.0.0-SNAPSHOT, 1.1.0)');

  const IN_RANGE = new Version('1.0.1');
  const OUT_RANGE = new Version('1.2.0');
  const EDGE_RANGE = new Version('1.1.0');
  const RELEASE_VERSION = new Version('1.0.0');
  const SNAPSHOT_VERSION = new Version('1.0.0-SNAPSHOT');

  it('should detect a version is in range', () => {
    expect(EXCLUSIVE_RANGE.versionIsInRange(IN_RANGE)).toBe(true);
  });

  it('should detect a version out of range', () => {
    expect(EXCLUSIVE_RANGE.versionIsInRange(OUT_RANGE)).toBe(false);
  });

  it('should detect edge range version', () => {
    expect(EXCLUSIVE_RANGE.versionIsInRange(EDGE_RANGE)).toBe(false);
    expect(INCLUSIVE_RANGE.versionIsInRange(EDGE_RANGE)).toBe(true);
  });

  it('should correctly determine snapshot range', () => {
    expect(EXCLUSIVE_SNAPSHOT_RANGE.versionIsInRange(RELEASE_VERSION)).toBe(true);
    expect(EXCLUSIVE_RANGE.versionIsInRange(SNAPSHOT_VERSION)).toBe(false);
  });
});
