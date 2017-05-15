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

import React from 'react';
import {mount} from 'enzyme';
import SamplerDropdown from 'components/DataPrep/SamplerDropdown';
jest.mock('api/dataprep');
console.warn = jest.genMockFunction();
console.trace = jest.genMockFunction();
console.error = jest.genMockFunction();
jest.useFakeTimers();
import DataPrepStore from 'components/DataPrep/store';
import DataPrepActions from 'components/DataPrep/store/DataPrepActions';
import {objectQuery} from 'services/helpers';
const workspaceObject = {
  "version": 1,
  "workspace": {
    "name": "b65933228e4a873a4f354ee3bbeb594d",
    "results": 100
  },
  "recipe": {
    "directives": [
      "parse-as-csv body , false"
    ]
  },
  "sampling": {
    "method": "FIRST",
    "limit": 1000
  },
  "properties": {
    "connection": "file",
    "file": "CustomerStream.csv",
    "path": "/path/CustomerStream.csv",
    "uri": "file:/path/CustomerStream.csv",
    "sampler": "poisson"
  }
};


const setWorkspace = (workspaceInfo = workspaceObject) => {
  let directives = objectQuery(workspaceInfo, 'recipe', 'directives') || [];
  let workspaceUri = objectQuery(workspaceInfo, 'properties', 'path');
  DataPrepStore.dispatch({
    type: DataPrepActions.setWorkspace,
    payload: {
      headers: {},
      data: [],
      directives,
      workspaceUri,
      workspaceInfo
    }
  });
};

describe('Unit tests for SamplerDropdown', () => {
  setWorkspace();
  it('Should render', () => {
    let samplerdropdown = mount(
      <SamplerDropdown />
    );
    jest.runAllTimers();
    expect(samplerdropdown.find('.dataprep-sampler-dropdown').length).toBe(1);
    expect(samplerdropdown.find('.dataprep-sampler-dropdown .icon-refresh').length).toBe(1);
    expect(samplerdropdown.find('.dropdown').length).toBe(1);
    samplerdropdown.unmount();
  });

  it('Should choose right options', () => {
    let samplerdropdown = mount(
      <SamplerDropdown />
    );
    jest.runAllTimers();
    let samplerMethod = samplerdropdown.state('samplerMethod');
    expect(samplerdropdown.find('.dropdown').length).toBe(1);
    expect(samplerdropdown.find('.dropdown .dropdown-item').length).toBe(4);
    expect(samplerdropdown.find('.selected.dropdown-item').length).toBe(1);
    expect(samplerMethod.name).toBe('poisson');

    let newWorkspace = Object.assign({}, workspaceObject,{
      properties: {
        "connection": "file",
        "file": "CustomerStream.csv",
        "path": "/path/CustomerStream.csv",
        "uri": "file:/path/CustomerStream.csv",
        "sampler": "bernoulli"
      }
    });
    setWorkspace(newWorkspace);
    samplerMethod = samplerdropdown.state('samplerMethod');
    expect(samplerMethod.name).toBe('bernoulli');
  });
});
