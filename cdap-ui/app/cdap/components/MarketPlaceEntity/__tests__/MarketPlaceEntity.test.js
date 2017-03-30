/*
 * Copyright © 2016 Cask Data, Inc.
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
import shortid from 'shortid';
import {mount} from 'enzyme';

jest.useFakeTimers();

jest.mock('api/market');
jest.mock('api/stream');
jest.mock('api/userstore');
jest.mock('api/pipeline');
jest.mock('api/namespace');
jest.mock('api/artifact');
import {MyMarketApi} from 'api/market';

import MarketPlaceEntity from 'components/MarketPlaceEntity';

const entity = {
  name: 'SampleEntityName',
  version: '1.0.0',
  label: 'Sample Entity Label',
  author: 'test',
  description: 'This is a test entity',
  org: 'Cask',
  created: Date.now(),
  cdapVersion: '4.1.1-SNAPSHOT'
};
const entityDetail = {
  "specVersion": "1.0",
  "label": "Access Log Sample",
  "description": "Sample access logs in Combined Log Format (CLF)",
  "author": "Cask",
  "org": "Cask Data, Inc.",
  "created": 1473901763,
  "categories": ["datapack"],
  "cdapVersion": "[4.1.0,4.2.0)",
  "actions": [{
      "type": "load_datapack",
      "label": "Access Logs",
      "arguments": [{
          "name": "name",
          "value": "accessLogs"
      }, {
          "name": "files",
          "value": ["access.txt"]
      }]
  }]
};
const entityDetail2 = {
  "specVersion": "1.0",
  "label": "Access Log Sample",
  "description": "Sample access logs in Combined Log Format (CLF)",
  "author": "Cask",
  "org": "Cask Data, Inc.",
  "created": 1473901763,
  "categories": ["datapack"],
  "cdapVersion": "[4.1.0,4.2.0)",
  "actions": [{
      "type": "load_datapack",
      "label": "Access Logs",
      "arguments": [{
          "name": "name",
          "value": "accessLogs"
      }, {
          "name": "files",
          "value": ["access.txt"]
      }]
  },{
      "type": "load_datapack",
      "label": "Access Logs",
      "arguments": [{
          "name": "name",
          "value": "accessLogs"
      }, {
          "name": "files",
          "value": ["access.txt"]
      }]
  },
  {
      "type": "load_datapack",
      "label": "Access Logs",
      "arguments": [{
          "name": "name",
          "value": "accessLogs"
      }, {
          "name": "files",
          "value": ["access.txt"]
      }]
  }]
};

const entityId = shortid.generate();

describe('MarketplaceEntity Unit tests', () => {
  let marketPlaceEntity;
  beforeEach(() => {
    marketPlaceEntity = mount(
      <MarketPlaceEntity
        entity={entity}
        entityId={entityId}
      />
    );
  });
  afterEach(() => {
    marketPlaceEntity.unmount();
  });
  it('Should render', () => {
    expect(marketPlaceEntity.find('.market-place-package-card').length).toBe(1);
    expect(marketPlaceEntity.find('.market-place-package-card .card-body').length).toBe(1);
    expect(marketPlaceEntity.find('.market-place-package-card .card-body .package-icon-container').length).toBe(1);
  });
  it('Should have appropriate state', () => {
    let instance = marketPlaceEntity.instance();
    expect(instance.state.expandedMode).toBe(false);
    expect(typeof instance.state.entityDetail).toBe('object');
    expect(instance.state.performSingleAction).toBe(false);
    expect(instance.props.entity).toBe(entity);
    expect(instance.props.entityId).toBe(entityId);
  });
  it('Should show the overlay on click', () => {
    let packageContainer = marketPlaceEntity.find('.market-place-package-card .cask-card');
    MyMarketApi.__setEntityDetail(entityDetail);
    packageContainer.simulate('click');
    jest.runAllTimers();
    marketPlaceEntity.update();
    expect(marketPlaceEntity.find('.market-place-package-card .expanded').length).toBe(1);

  });
  it('Should update the state appropriately on click', () => {
    let packageContainer = marketPlaceEntity.find('.market-place-package-card .cask-card');
    MyMarketApi.__setEntityDetail(entityDetail);
    packageContainer.simulate('click');
    jest.runAllTimers();
    marketPlaceEntity.update();
    expect(marketPlaceEntity.state('expandedMode')).toBe(true);
    expect(marketPlaceEntity.state('entityDetail')).toBe(entityDetail);
    expect(marketPlaceEntity.state('entityDetail').actions.length).toBe(1);
  });
  it('Should show appropriate overlay template for multiple actions', () => {
    let packageContainer = marketPlaceEntity.find('.market-place-package-card .cask-card');
    MyMarketApi.__setEntityDetail(entityDetail2);
    packageContainer.simulate('click');
    jest.runAllTimers();
    marketPlaceEntity.update();
    expect(marketPlaceEntity.find('.market-place-package-card .expanded').length).toBe(1);
    expect(marketPlaceEntity.find('.market-place-package-card .expanded .market-entity-actions').length).toBe(1);
    expect(marketPlaceEntity.find('.market-place-package-card .expanded .market-entity-actions .action-container').length).toBe(3);
  });
  it('Should update the state appropriately on click for multiple actions container', () => {
    let packageContainer = marketPlaceEntity.find('.market-place-package-card .cask-card');
    MyMarketApi.__setEntityDetail(entityDetail2);
    packageContainer.simulate('click');
    jest.runAllTimers();
    marketPlaceEntity.update();
    expect(marketPlaceEntity.state('expandedMode')).toBe(true);
    expect(marketPlaceEntity.state('entityDetail')).toBe(entityDetail2);
    expect(marketPlaceEntity.state('entityDetail').actions.length).toBe(3);
  });
  it('Should handle clicking on multiple MarketPlaceEntities', () => {
    let marketPlaceEntity1 = mount(
      <MarketPlaceEntity
        entity={entity}
        entityId={shortid.generate()}
      />
    );

    let packageContainer = marketPlaceEntity.find('.market-place-package-card .cask-card');
    MyMarketApi.__setEntityDetail(entityDetail);
    packageContainer.simulate('click');
    jest.runAllTimers();
    marketPlaceEntity.update();
    marketPlaceEntity1.update();

    expect(marketPlaceEntity.find('.market-place-package-card .expanded').length).toBe(1);
    expect(marketPlaceEntity1.find('.market-place-package-card .expanded').length).toBe(0);

    let packageContainer1 = marketPlaceEntity1.find('.market-place-package-card .cask-card');
    packageContainer1.simulate('click');
    jest.runAllTimers();
    marketPlaceEntity.update();
    marketPlaceEntity1.update();

    expect(marketPlaceEntity.find('.market-place-package-card .expanded').length).toBe(0);
    expect(marketPlaceEntity1.find('.market-place-package-card .expanded').length).toBe(1);
  });
});
