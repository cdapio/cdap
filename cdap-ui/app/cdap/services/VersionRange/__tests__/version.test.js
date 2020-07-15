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
jest.disableAutomock();

describe('Version Class', () => {
  const SNAPSHOT_VERSION = '1.0.0-SNAPSHOT';
  const RELEASE_VERSION = '1.0.0';
  const MINOR_VERSION = '1.1.0';
  const FIX_VERSION = '1.0.1';
  const MINOR_FIX_VERSION = '1.1.1';

  it('should detect SNAPSHOT version', () => {
    let snapshotVersion = new Version(SNAPSHOT_VERSION);
    let releaseVersion = new Version(RELEASE_VERSION);

    expect(snapshotVersion.isSnapshot()).toBe(true);
    expect(releaseVersion.isSnapshot()).toBe(false);
  });

  it('should correctly compare same version', () => {
    let version1 = new Version(RELEASE_VERSION);
    let version2 = new Version(RELEASE_VERSION);

    let snapshot1 = new Version(SNAPSHOT_VERSION);
    let snapshot2 = new Version(SNAPSHOT_VERSION);

    expect(version1.compareTo(version2)).toBe(0);
    expect(snapshot1.compareTo(snapshot2)).toBe(0);
  });

  it('should correctly compare release and snasphot', () => {
    let snapshotVersion = new Version(SNAPSHOT_VERSION);
    let releaseVersion = new Version(RELEASE_VERSION);

    expect(snapshotVersion.compareTo(releaseVersion)).toBe(-1);
    expect(releaseVersion.compareTo(snapshotVersion)).toBe(1);
  });

  it('should correctly compare minor version', () => {
    let snapshotVersion = new Version(SNAPSHOT_VERSION);
    let releaseVersion = new Version(RELEASE_VERSION);
    let minorVersion = new Version(MINOR_VERSION);

    expect(releaseVersion.compareTo(minorVersion)).toBe(-1);
    expect(minorVersion.compareTo(releaseVersion)).toBe(1);
    expect(minorVersion.compareTo(snapshotVersion)).toBe(1);
    expect(snapshotVersion.compareTo(minorVersion)).toBe(-1);
  });

  it('should correctly compare fix version', () => {
    let snapshotVersion = new Version(SNAPSHOT_VERSION);
    let releaseVersion = new Version(RELEASE_VERSION);
    let fixVersion = new Version(FIX_VERSION);

    expect(releaseVersion.compareTo(fixVersion)).toBe(-1);
    expect(fixVersion.compareTo(releaseVersion)).toBe(1);
    expect(fixVersion.compareTo(snapshotVersion)).toBe(1);
    expect(snapshotVersion.compareTo(fixVersion)).toBe(-1);
  });

  it('should correctly compare fix version', () => {
    let snapshotVersion = new Version(SNAPSHOT_VERSION);
    let fixVersion = new Version(FIX_VERSION);
    let minorFixVersion = new Version(MINOR_FIX_VERSION);

    expect(fixVersion.compareTo(minorFixVersion)).toBe(-1);
    expect(minorFixVersion.compareTo(fixVersion)).toBe(1);
    expect(minorFixVersion.compareTo(snapshotVersion)).toBe(1);
    expect(snapshotVersion.compareTo(minorFixVersion)).toBe(-1);
  });
});
