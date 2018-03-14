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
import {shallow} from 'enzyme';
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


import Overview from 'components/Overview';
import SearchStoreActions from 'components/EntityListView/SearchStore/SearchStoreActions';
import SearchStore from 'components/EntityListView/SearchStore';
import {MyMetadataApi} from 'api/metadata';
console.warn = jest.genMockFunction();
console.trace = jest.genMockFunction();
console.error = jest.genMockFunction();
jest.useFakeTimers();

describe('Unit tests for Overview component', () => {
  it('Should render', () => {
    let overview = shallow(
      <Overview />
    );
    expect(overview.find('.overview-container').length).toBe(1);
    SearchStore.dispatch({
      type: SearchStoreActions.SETOVERVIEWENTITY,
      payload: {
        overviewEntity: {
          "id": "TestPipeline",
          "type": "application"
        }
      }
    });
    expect(overview.find('.overview-container.show-overview').length).toBe(1);
    expect(overview.find('.overview-wrapper').length).toBe(1);
    expect(overview.find('.overview-wrapper .fa.fa-spinner').length).toBe(1);
  });

  it('Should render with an application overview', () => {
    let overview = shallow(
      <Overview />
    );
    MyMetadataApi.__setMetadata({});
    SearchStore.dispatch({
      type: SearchStoreActions.SETOVERVIEWENTITY,
      payload: {
        overviewEntity: {
          "id": "TestPipeline",
          "type": "application"
        }
      }
    });
    jest.runAllTimers();
    let {loading, showOverview, entity, tag} = overview.state();
    expect(loading).toBe(false);
    expect(showOverview).toBe(true);
    expect(entity.id).toBe('TestPipeline');
    expect(entity.type).toBe('application');
    expect(tag.name).toBe('AppOverview');
  });

  it('Should render with a stream overview', () => {
    let overview = shallow(
      <Overview />
    );
    MyMetadataApi.__setMetadata({});
    SearchStore.dispatch({
      type: SearchStoreActions.SETOVERVIEWENTITY,
      payload: {
        overviewEntity: {
          "id": "TestStream",
          "type": "stream"
        }
      }
    });
    jest.runAllTimers();
    let {loading, showOverview, entity, tag} = overview.state();
    expect(loading).toBe(false);
    expect(showOverview).toBe(true);
    expect(entity.id).toBe('TestStream');
    expect(entity.type).toBe('stream');
    expect(tag.name).toBe('StreamOverview');
  });

  it('Should render with a dataset overview', () => {
    let overview = shallow(
      <Overview />
    );
    MyMetadataApi.__setMetadata({});
    SearchStore.dispatch({
      type: SearchStoreActions.SETOVERVIEWENTITY,
      payload: {
        overviewEntity: {
          "id": "TestDataset",
          "type": "dataset"
        }
      }
    });
    jest.runAllTimers();
    let {loading, showOverview, entity, tag} = overview.state();
    expect(loading).toBe(false);
    expect(showOverview).toBe(true);
    expect(entity.id).toBe('TestDataset');
    expect(entity.type).toBe('dataset');
    expect(tag.name).toBe('DatasetOverview');
  });

  it('Should render a right 404 view', () => {
    let overview = shallow(
      <Overview />
    );
    SearchStore.dispatch({
      type: SearchStoreActions.SETOVERVIEWENTITY,
      payload: {
        overviewEntity: {
          "id": "TestPipeline",
          "type": "application"
        }
      }
    });
    MyMetadataApi.__setMetadata({
      statusCode: 404
    }, true);
    jest.runAllTimers();
    expect(overview.find('.overview-error-container').length).toBe(1);
    expect(overview.find('.overview-error-container h4 strong').text()).toBe('features.Overview.errorMessage404');
    expect(overview.find('.overview-error-container .message-container > span').text()).toBe('features.EntityListView.emptyMessage.suggestion');
    let {errorContent} = overview.state();
    expect(errorContent).not.toBe(null);
  });

  it('Should render a right Un-Authorized message', () => {
    let overview = shallow(
      <Overview />
    );
    SearchStore.dispatch({
      type: SearchStoreActions.SETOVERVIEWENTITY,
      payload: {
        overviewEntity: {
          "id": "TestPipeline",
          "type": "application"
        }
      }
    });
    MyMetadataApi.__setMetadata({
      statusCode: 403
    }, true);
    jest.runAllTimers();
    expect(overview.find('.overview-error-container').length).toBe(1);
    expect(overview.find('.overview-error-container h4 strong').text()).toBe('features.Overview.errorMessageAuthorization');
    expect(overview.find('.overview-error-container .message-container > span').text()).toBe('features.EntityListView.emptyMessage.suggestion');
  });
});
