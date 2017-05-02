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

import DatasetOverview from 'components/Overview/DatasetOverview';
import {MyMetadataApi} from 'api/metadata';
import {MyDatasetApi} from 'api/dataset';
import OverviewHeader from 'components/Overview/OverviewHeader';
import OverviewMetaSection from 'components/Overview/OverviewMetaSection';
import DatasetOverviewTab from 'components/Overview/DatasetOverview/DatasetOverviewTab';
jest.useFakeTimers();


describe('Unit test for DatasetOverview', () => {
  it('Should render', () => {
    let datasetoverview = shallow(
      <DatasetOverview />
    );
    expect(datasetoverview.find('.fa.fa-spinner').length).toBe(1);
  });
  it('Should render the entity if provided', () => {
    MyMetadataApi.__setProperties({
      schema: 'mySchema'
    });
    MyDatasetApi.__setPrograms([{
      id: 'program1',
      application: {
        applicationId: 'MyApp1'
      }
    }]);
    let entity = {
      id: 'MyApp1'
    };
    let datasetoverview = shallow(
      <DatasetOverview entity={entity}/>
    );
    jest.runAllTimers();
    let {entityDetail} = datasetoverview.state();
    expect(entityDetail.id).toBe(entity.id);
    expect(entityDetail.schema).toBe('mySchema');
    expect(datasetoverview.find('.app-overview').length).toBe(1);
    expect(datasetoverview.find(OverviewHeader).length).toBe(1);
    expect(datasetoverview.find(OverviewHeader).props().title).toBe('commons.entity.dataset.singular');
    expect(datasetoverview.find(OverviewMetaSection).length).toBe(1);
    expect(datasetoverview.find(DatasetOverviewTab).length).toBe(1);
  });
  it('Should update on new entity', () => {
    MyMetadataApi.__setProperties({});
    MyDatasetApi.__setPrograms([{
      id: 'program1',
      application: {
        applicationId: 'MyApp1'
      }
    }]);
    let entity = {
      id: 'MyDataset1'
    };
    let datasetoverview = shallow(
      <DatasetOverview entity={entity}/>
    );
    jest.runAllTimers();
    let {entityDetail} = datasetoverview.state();
    expect(entityDetail.id).toBe(entity.id);
    datasetoverview.setProps({
      entity: {
        id: 'MyDataset2'
      }
    });
    jest.runAllTimers();
    entityDetail = datasetoverview.state().entityDetail;
    expect(entityDetail.id).toBe('MyDataset2');
  });
});
