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

import LoadigIndicator from 'components/LoadingIndicator';
import LoadingIndicatorStore, {LOADINGSTATUS} from 'components/LoadingIndicator/LoadingIndicatorStore';

let loadingIndicator;
let defaultMessage = 'Loading Message 1';
let defaultSubtitle = 'Loading Message Subtitle';

const showLoadingIndicator = (message, subtitle) => (
  LoadingIndicatorStore.dispatch({
    type: 'STATUSUPDATE',
    payload: {
      status: LOADINGSTATUS.SHOWLOADING,
      message: message || defaultMessage,
      subtitle: subtitle || defaultSubtitle
    }
  })
);
const hideLoadingIndicator = () => (
  LoadingIndicatorStore.dispatch({
    type: 'STATUSUPDATE',
    payload: {
      status: LOADINGSTATUS.HIDELOADING
    }
  })
);
describe('LoadingIndicator Unit Tests', () => {
  beforeEach(() => {
    jasmine.clock().install();
    loadingIndicator = mount(
      <LoadigIndicator />
    );
  });

  afterEach(() => {
    hideLoadingIndicator();
    jasmine.clock().tick(300);
    jasmine.clock().uninstall();
    loadingIndicator.unmount();
  });

  it('Should render', () => {
    showLoadingIndicator();
    jasmine.clock().tick(500);
    expect(document.getElementsByClassName('modal').length).toBe(1);
    expect(document.getElementsByClassName('loading-indicator').length).toBe(1);
  });

  it('Should update state appropriately', () => {
    showLoadingIndicator();
    jasmine.clock().tick(500);
    expect(loadingIndicator.state().showLoading).toBe(true);
    expect(loadingIndicator.state().message).toBe(defaultMessage);
    expect(loadingIndicator.state().subtitle).toBe(defaultSubtitle);
  });

  it('Should update message and subtitle', () => {
    showLoadingIndicator();
    jasmine.clock().tick(500);
    expect(loadingIndicator.state().showLoading).toBe(true);
    expect(loadingIndicator.state().message).toBe(defaultMessage);
    expect(loadingIndicator.state().subtitle).toBe(defaultSubtitle);
    let newMessage = 'LoadingIndicator Message 2';
    let newSubtitle = 'LoadingIndicator Subtitle Message 2';
    showLoadingIndicator(newMessage, newSubtitle);
    expect(loadingIndicator.state().message).toBe(newMessage);
    expect(loadingIndicator.state().subtitle).toBe(newSubtitle);
  });

  it('Should close on update in state', () => {
    showLoadingIndicator();
    jasmine.clock().tick(500);
    hideLoadingIndicator();
    jasmine.clock().tick(500);
    expect(document.getElementsByClassName('modal').length).toBe(0);
    expect(document.getElementsByClassName('loading-indicator').length).toBe(0);
  });

  it('Should not render default icon', () => {
    showLoadingIndicator();
    expect(loadingIndicator.props().icon).toBe('');
  });

  it('Should render icon passed as props', () => {
    let customIcon = "icon-hydrator";
    let loading = mount(
      <LoadigIndicator
        icon={customIcon}
      />
    );
    expect(loading.props().icon).toBe(customIcon);
  });
});
