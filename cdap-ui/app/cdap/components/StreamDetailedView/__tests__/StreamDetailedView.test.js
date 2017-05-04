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
import StreamDetailedView from 'components/StreamDetailedView';
import {mount} from 'enzyme';
import {MemoryRouter, Route} from 'react-router-dom';
import {MyMetadataApi} from 'api/metadata';
import {MyStreamApi} from 'api/stream';
import {MyProgramApi} from 'api/program';

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
jest.mock('reactstrap', () => {
  const RealModule = require.requireActual('reactstrap');
  const MyModule = Object.assign({}, RealModule, { 'Tooltip': 'Tooltip'});
  return MyModule;
});
console.warn = jest.genMockFunction();
console.trace = jest.genMockFunction();
console.error = jest.genMockFunction();
jest.useFakeTimers();
const streamProperties = {
  "programs": [
    {
      "application": {
        "namespace": {
          "id": "default"
        },
        "applicationId": "dataprep"
      },
      "type": "Service",
      "id": "service",
      "uniqueId": "rkfgNzP3Cl",
      "app": "dataprep",
      "name": "service"
    }
  ],
  "schema": "",
  "name": "MyApp1",
  "app": "MyApp1",
  "id": "recipes",
  "type": "datasetinstance",
  "properties": {
    "schema": "",
    "creation-time": "1492442466016",
    "type": "co.cask.cdap.api.dataset.lib.ObjectMappedTable",
    "entity-name": "recipes"
  }
};
const streamPrograms = [{
  id: 'program1',
  "type": "Service",
  application: {
    applicationId: 'MyApp1'
  }
}];
const programStatus = {
  status: 'RUNNING'
};
const programRunRecords = [
  {
    "runid": "fd9138a1-2a07-11e7-be3d-42010a800009",
    "start": 1493159946,
    "status": "RUNNING",
    "properties": {
      "runtimeArgs": "{\"logical.start.time\":\"1493159946525\"}"
    }
  }
];
window.getTrackerUrl = jest.fn();
window.getHydratorUrl = jest.fn();
describe('Unit tests for StreamDetailedView', () => {
  it('Should render a valid stream', () => {
    MyMetadataApi.__setProperties(streamProperties);
    MyMetadataApi.__setTags([]);
    MyStreamApi.__setPrograms(streamPrograms);
    let entity = {
      id: 'purchaseStream'
    };
    const RouterRender = (match) => {
      return <StreamDetailedView match={match.match} location={match.location} entity={entity} />;
    };
    let streamDetailedView = mount(
      <MemoryRouter initialEntries={['/ns/default/streams/purchaseStream/programs']}>
        <Route path="/ns/:namespace/streams/:streamId" render={RouterRender} />
      </MemoryRouter>
    );
    jest.runAllTimers();
    expect(streamDetailedView.find('.streams-detailed-view').length).toBe(1);
    expect(streamDetailedView.find('.streams-detailed-view .overview-meta-section').length).toBe(1);
    expect(streamDetailedView.find('.streams-detailed-view .overview-meta-section h2').props().title).toBe('purchaseStream');
  });
  it('Should render individual tabs', () => {
    MyMetadataApi.__setProperties(streamProperties);
    MyMetadataApi.__setTags([]);
    MyStreamApi.__setPrograms(streamPrograms);
    MyProgramApi.setRunRecords(programRunRecords);
    MyProgramApi.setProgramStatus(programStatus);
    let entity = {
      id: 'purchaseStream'
    };
    const RouterRender = (match) => {
      return <StreamDetailedView match={match.match} location={match.location} entity={entity} />;
    };
    const getActiveLinkFromTab = (el) => {
      let tabs = el.find('.nav.nav-tabs').at(0);
      let navItems = tabs.find('.nav-item');
      let activeLink = navItems.find('.active');
      return activeLink;
    };
    let streamProgramsTab = mount(
      <MemoryRouter initialEntries={['/ns/default/streams/purchaseStream/programs']}>
        <Route path="/ns/:namespace/streams/:streamId" render={RouterRender} />
      </MemoryRouter>
    );
    jest.runAllTimers();
    let activeLink = getActiveLinkFromTab(streamProgramsTab);
    expect(activeLink.length).toBe(1);
    expect(activeLink.prop('href')).toBe('/ns/default/streams/purchaseStream/programs');

    let streamUsageTab = mount(
      <MemoryRouter initialEntries={['/ns/default/streams/purchaseStream']}>
        <Route path="/ns/:namespace/streams/:streamId" render={RouterRender} />
      </MemoryRouter>
    );
    jest.runAllTimers();
    let usageTabActiveLink = getActiveLinkFromTab(streamUsageTab);
    expect(usageTabActiveLink.prop('href')).toBe('/ns/default/streams/purchaseStream/usage');

    let streamPropertiesTab = mount(
      <MemoryRouter initialEntries={['/ns/default/streams/purchaseStream/properties']}>
        <Route path="/ns/:namespace/streams/:streamId" render={RouterRender} />
      </MemoryRouter>
    );
    MyMetadataApi.__setProperties(streamProperties.properties);
    jest.runAllTimers();
    let propertiesTabActiveLink = getActiveLinkFromTab(streamPropertiesTab);
    expect(propertiesTabActiveLink.prop('href')).toBe('/ns/default/streams/purchaseStream/properties');
  });
  it('Should render a proper 404 page', () => {
    let errorMessage = `Stream 'purchaseStream' not found`;
    MyMetadataApi.__setProperties({
      statusCode: 404,
      message: errorMessage
    }, true);
    let entity = {
      id: 'purchaseStream'
    };
    const RouterRender = (match) => {
      return <StreamDetailedView match={match.match} location={match.location} entity={entity} />;
    };
    let streamTabs = mount(
      <MemoryRouter initialEntries={['/ns/default/streams/purchaseStream/properties']}>
        <Route path="/ns/:namespace/streams/:streamId" render={RouterRender} />
      </MemoryRouter>
    );
    jest.runAllTimers();
    expect(streamTabs.find('.page-not-found').length).toBe(1);
    expect(streamTabs.find('.page-not-found img').prop('src')).toBe('/cdap_assets/img/404.png');
  });
});
