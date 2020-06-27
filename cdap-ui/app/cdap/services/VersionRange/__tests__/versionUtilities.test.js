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

import { findHighestVersion, findLowestVersion } from 'services/VersionRange/VersionUtilities';
jest.disableAutomock();

describe('Version Utilities', () => {
  const versionsArray = [
    '1.0.0',
    '1.2.0-SNAPSHOT',
    '1.1.0',
    '1.2.1',
    '1.1.5'
  ];

  it('should find highest version', () => {
    expect(findHighestVersion(versionsArray, true)).toBe('1.2.1');
  });

  it('should find lowest version', () => {
    expect(findLowestVersion(versionsArray, true)).toBe('1.0.0');
  });
});
