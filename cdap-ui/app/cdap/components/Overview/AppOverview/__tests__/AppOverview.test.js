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

import AppOverview from 'components/Overview/AppOverview';
import {MyMetadataApi} from 'api/metadata';
import {MyAppApi} from 'api/app';
import OverviewHeader from 'components/Overview/OverviewHeader';
import OverviewMetaSection from 'components/Overview/OverviewMetaSection';
import AppOverviewTab from 'components/Overview/AppOverview/AppOverviewTab';
jest.useFakeTimers();


describe('Unit test for AppOverview', () => {
  it('Should render', () => {
    let appoverview = shallow(
      <AppOverview />
    );
    expect(appoverview.find('.fa.fa-spinner').length).toBe(1);
  });
  it('Should render the entity if provided', () => {
    MyMetadataApi.__setProperties({});
    MyAppApi.__setApp({
      programs: [{name: 'program1'}],
      streams: [{name: 'stream1'}],
      datasets: [{name: 'dataset1'}]
    });
    let entity = {
      id: 'MyApp1'
    };
    let appoverview = shallow(
      <AppOverview entity={entity}/>
    );
    jest.runAllTimers();
    let {entityDetail} = appoverview.state();
    expect(entityDetail.id).toBe(entity.id);
    expect(appoverview.find('.app-overview').length).toBe(1);
    expect(appoverview.find(OverviewHeader).length).toBe(1);
    expect(appoverview.find(OverviewHeader).props().title).toBe('commons.entity.application.singular');
    expect(appoverview.find(OverviewMetaSection).length).toBe(1);
    expect(appoverview.find(AppOverviewTab).length).toBe(1);
  });
  it('Should update on new entity', () => {
    MyMetadataApi.__setProperties({});
    MyAppApi.__setApp({
      programs: [{name: 'program1'}],
      streams: [{name: 'stream1'}],
      datasets: [{name: 'dataset1'}]
    });
    let entity = {
      id: 'MyApp1'
    };
    let appoverview = shallow(
      <AppOverview entity={entity}/>
    );
    jest.runAllTimers();
    let {entityDetail} = appoverview.state();
    expect(entityDetail.id).toBe(entity.id);
    appoverview.setProps({
      entity: {
        id: 'MyApp2'
      }
    });
    jest.runAllTimers();
    entityDetail = appoverview.state().entityDetail;
    expect(entityDetail.id).toBe('MyApp2');
  });
});
