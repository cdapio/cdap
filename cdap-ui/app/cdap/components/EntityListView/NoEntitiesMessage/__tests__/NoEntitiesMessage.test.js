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
import { mount } from 'enzyme';
import NoEntitiesMessage from 'components/EntityListView/NoEntitiesMessage';
import PlusButtonStore from 'services/PlusButtonStore';

const defaultEmptyMessagePath = 'features.EntityListView.emptyMessage.default';
const filterEmptyMessagePath = 'features.EntityListView.emptyMessage.filter';
const searchEmptyMessagePath = 'features.EntityListView.emptyMessage.search';
let noEntitiesMessage;

let filtersAreApplied;

describe('NoEntitiesMessage Unit tests - ', () => {
  beforeEach(() => {
    filtersAreApplied = jest.fn();
  });
  it('Should render', () => {
    filtersAreApplied.mockReturnValueOnce(false);
    noEntitiesMessage = mount(
      <NoEntitiesMessage
        searchText=''
        filtersAreApplied={filtersAreApplied}
      />
    );
    expect(noEntitiesMessage.find('.empty-message-container').length).toBe(1);
    expect(noEntitiesMessage.find('.empty-message-container strong').length).toBe(1);
    expect(noEntitiesMessage.find('.empty-message-container .empty-message-suggestions').length).toBe(1);
    noEntitiesMessage.unmount();
  });

  it('Should show appropriate default message', () => {
    filtersAreApplied.mockReturnValueOnce(false);
    noEntitiesMessage = mount(
      <NoEntitiesMessage
        searchText='*'
        filtersAreApplied={filtersAreApplied}
      />
    );
    expect(noEntitiesMessage.find('.empty-message-container strong').text()).toBe(defaultEmptyMessagePath);
    expect(noEntitiesMessage.find('.empty-message-container .action-item.clear').length).toBe(0);
    expect(noEntitiesMessage.find('.empty-message-container .action-item.add-entity').length).toBe(1);
    noEntitiesMessage.unmount();
  });

  it('Should show appropriate message when filters are applied', () => {
    filtersAreApplied.mockReturnValueOnce(true);
    noEntitiesMessage = mount(
      <NoEntitiesMessage
        searchText='*'
        filtersAreApplied={filtersAreApplied}
      />
    );
    expect(noEntitiesMessage.find('.empty-message-container strong').text()).toBe(filterEmptyMessagePath);
    expect(noEntitiesMessage.find('.empty-message-container .action-item.clear').length).toBe(1);
    expect(noEntitiesMessage.find('.empty-message-container .action-item.add-entity').length).toBe(1);
    noEntitiesMessage.unmount();
  });

  it('Should show appropriate message when there is a search query', () => {
    filtersAreApplied.mockReturnValueOnce(false);
    noEntitiesMessage = mount(
      <NoEntitiesMessage
        searchText='abc'
        filtersAreApplied={filtersAreApplied}
      />
    );
    expect(noEntitiesMessage.find('.empty-message-container strong').text()).toBe(searchEmptyMessagePath);
    expect(noEntitiesMessage.find('.empty-message-container .action-item.clear').length).toBe(1);
    expect(noEntitiesMessage.find('.empty-message-container .action-item.add-entity').length).toBe(1);
    noEntitiesMessage.unmount();
  });

  it('Should show appropriate message when both filters and search query are applied', () => {
    filtersAreApplied.mockReturnValueOnce(true);
    noEntitiesMessage = mount(
      <NoEntitiesMessage
        searchText='abc'
        filtersAreApplied={filtersAreApplied}
      />
    );
    expect(noEntitiesMessage.find('.empty-message-container strong').text()).toBe(searchEmptyMessagePath);
    expect(noEntitiesMessage.find('.empty-message-container .action-item.clear').length).toBe(1);
    expect(noEntitiesMessage.find('.empty-message-container .action-item.add-entity').length).toBe(1);
    noEntitiesMessage.unmount();
  });

  it('Should open up the Add Entity modal when "Add" is clicked', () => {
    filtersAreApplied.mockReturnValueOnce(false);
    noEntitiesMessage = mount(
      <NoEntitiesMessage
        searchText=''
        filtersAreApplied={filtersAreApplied}
      />
    );
    let addButtonContainer = noEntitiesMessage.find('.empty-message-container .action-item.add-entity');
    expect(PlusButtonStore.getState().modalState.openModal).toBe(false);
    addButtonContainer.simulate('click');
    expect(PlusButtonStore.getState().modalState).toBe(true);
    noEntitiesMessage.unmount();
  });

  it('Should call function to clear search and filters when "Clear" is clicked', () => {
    filtersAreApplied.mockReturnValueOnce(true);
    let clearSearchAndFilters = jest.fn();
    noEntitiesMessage = mount(
      <NoEntitiesMessage
        searchText=''
        filtersAreApplied={filtersAreApplied}
        clearSearchAndFilters={clearSearchAndFilters}
      />
    );
    let clearButtonContainer = noEntitiesMessage.find('.empty-message-container .action-item.clear');
    clearButtonContainer.simulate('click');
    expect(clearSearchAndFilters.mock.calls.length).toBe(1);
    noEntitiesMessage.unmount();
  });
});
