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

import {getArtifactNameAndVersion} from 'services/helpers';

describe('Unit Tests for Helpers: "getArtifactNameAndVersion"', () => {

  it('Usecase 1 - Valid: Should return correct name & version', () => {
    var jarfileName = 'wrangler-service-1.3.0-SNAPSHOT';
    let {name, version} = getArtifactNameAndVersion(jarfileName);
    expect(name).toBe('wrangler-service');
    expect(version).toBe('1.3.0-SNAPSHOT');
  });

  it('Usecase 2 - Invalid: Should return undefined for version if it could not find', () => {
    var jarfileName = 'invalid-file-name-without-a-version';
    let {name, version} = getArtifactNameAndVersion(jarfileName);
    expect(name).toBe(jarfileName);
    expect(version).toBe(undefined);
  });

  it('Usecase 3: Should ignore unnecessary patterns & return correct name, version', () => {
    var jarfileName = 'redshifttos3-action-plugin-1.0.0-SNAPSHOT';
    let {name, version} = getArtifactNameAndVersion(jarfileName);
    expect(name).toBe('redshifttos3-action-plugin');
    expect(version).toBe('1.0.0-SNAPSHOT');
  });

  it('Usecase 4: Should return undefined for name & version if provided with an undefined input', () => {
    let {name, version} = getArtifactNameAndVersion();
    expect(name).toBe(undefined);
    expect(version).toBe(undefined);
  });

  it('Usecase 5: Should return "" for name & undefined for version if provided with an undefined input', () => {
    let {name, version} = getArtifactNameAndVersion('');
    expect(name).toBe('');
    expect(version).toBe(undefined);
  });

  it('Usecase 6: Should return null for name & undefined for version if provided with an undefined input', () => {
    let {name, version} = getArtifactNameAndVersion(null);
    expect(name).toBe(null);
    expect(version).toBe(undefined);
  });

});
