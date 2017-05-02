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

import {mount} from 'enzyme';
import {MemoryRouter, Route} from 'react-router-dom';
import React from 'react';
import MyDataPrepApi from 'api/dataprep';
import FileBrowser from 'components/FileBrowser';

jest.mock('api/userstore');
jest.mock('api/search');
jest.mock('api/explore');
jest.mock('api/dataset');
jest.mock('api/app');
jest.mock('api/artifact');
jest.mock('api/metric');
jest.mock('api/program');
jest.mock('api/stream');
jest.mock('api/stream');
jest.mock('api/market');
jest.mock('api/preference');
jest.mock('api/pipeline');
jest.mock('api/namespace');
jest.mock('api/metadata');
jest.mock('api/dataprep');
jest.mock('reactstrap', () => {
 const RealModule = require.requireActual('reactstrap');
 const MyModule = Object.assign({}, RealModule, { 'Tooltip': 'Tooltip'});
 return MyModule;
});

console.warn = jest.genMockFunction();
console.trace = jest.genMockFunction();
console.error = jest.genMockFunction();
jest.useFakeTimers();

const explorerValues = [
  {
    "last-modified": "03/01/17 21:44",
    "directory": true,
    "name": "Shared",
    "owner": "root",
    "path": "/Users/Shared/",
    "wrangle": false,
    "permission": "rwxrwxrwx",
    "type": "Directory",
    "group": "wheel",
    "uri": "file:/Users/Shared/",
    "size": 204,
    "uniqueId": "SJFs9S1vyb",
    "displaySize": "204 B"
  }
];
describe('Unit tests for FileBrowser', () => {
  it('Should render', () => {
    MyDataPrepApi.__setExplorerValues({
      values: explorerValues
    });
    let fileBrowser = mount(
      <MemoryRouter initialEntries={['/ns/default/file']}>
        <Route exact path="/ns/:namespace/file" component={FileBrowser} />
      </MemoryRouter>
    );
    jest.runAllTimers();
    expect(fileBrowser.find('.file-browser-container').length).toBe(1);
    expect(fileBrowser.find('.sub-panel .path-container .file-path-container .active-directory').prop('href')).toBe('/ns/default/file/Users');
    expect(fileBrowser.find('.directory-content-table .content-body a').prop('href')).toBe('/ns/default/file/Users/Shared/');
    expect(fileBrowser.find('.directory-content-table .content-body a .content-row').children().length).toBe(7);
  });

  it('Should render nested subfolders with folded breadcrumb', () => {
    MyDataPrepApi.__setExplorerValues({
      values: explorerValues
    });
    let fileBrowser = mount(
      <MemoryRouter initialEntries={['/ns/default/file/Users/dummyuser/Path1/Subfolder1/SubSubFolder2']}>
        <Route path="/ns/:namespace/file" component={FileBrowser} />
      </MemoryRouter>
    );
    jest.runAllTimers();
    let collapsedPaths = `.sub-panel .path-container .file-path-container .collapsed-paths`;
    expect(fileBrowser.find('.file-browser-container').length).toBe(1);
    expect(fileBrowser.find(`${collapsedPaths}`).length).toBe(1);
    expect(fileBrowser.find(`${collapsedPaths} .collapsed-dropdown .collapsed-dropdown-toggle`).length).toBe(1);
    expect(fileBrowser.find(`.collapsed-dropdown-toggle button`).at(0).text()).toBe('...');
  });

  // FIXME: Right now unknown path (404 and 500) are not handled yet.
  // We should add unit tests as and when we add those logic.
  // Related JIRA: https://issues.cask.co/browse/CDAP-9447
});
