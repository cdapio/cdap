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
import Alert from 'components/Alert';

const defaultAlertMessage = "Alert Message 1";
let alert;

describe('Alert Unit tests', () => {
  beforeEach(() => {
    jasmine.clock().install();
    let alertType = 'success';
    let alertMessage = defaultAlertMessage;
    alert = mount(
      <Alert
        showAlert={true}
        type={alertType}
        message={alertMessage}
      />
    );
  });
  afterEach( () => {
    alert.unmount();
    jasmine.clock().uninstall();
  });

  it('Should render', () => {
    jasmine.clock().tick(500);
    expect(document.getElementsByClassName('modal').length).toBe(1);
    expect(document.getElementsByClassName('global-alert').length).toBe(1);
    expect(document.getElementsByClassName('message').length).toBe(1);
    expect(document.querySelector('.modal .global-alert .modal-content .fa')).not.toBe(null);
    expect(alert.props().showAlert).toBe(true);
    expect(alert.props().type).toBe('success');
  });

  it('Should have a valid state', () => {
    jasmine.clock().tick(500);
    expect(alert.state().message).toBe(defaultAlertMessage);
    expect(alert.state().type).toBe('success');
    expect(alert.state().showAlert).toBe(true);
  });

  it('Should have a valid state on alert type/message change', () => {
    let alertType = 'success';
    let alertMessage = defaultAlertMessage;
    jasmine.clock().tick(500);
    expect(alert.state().message).toBe(defaultAlertMessage);
    expect(alert.state().type).toBe(alertType);
    expect(alert.state().showAlert).toBe(true);

    alertType = 'error';
    alertMessage = 'Error Message';
    alert.setProps({type: alertType, message: alertMessage});

    jasmine.clock().tick(500);
    expect(alert.state().message).toBe(alertMessage);
    expect(alert.state().type).toBe(alertType);
    expect(alert.state().showAlert).toBe(true);

  });

  it('Should close when clicked on the close button', () => {
    jasmine.clock().tick(500);
    let closeBtn = document.querySelector('.global-alert .fa.fa-times');
    closeBtn.click();
    jasmine.clock().tick(500);
    expect(document.getElementsByClassName('global-alert').length).toBe(0);
  });

  it('Should close when 3s has elapsed since showing the alert', () => {
    setTimeout(() => {
      expect(document.getElementsByClassName('global-alert').length).toBe(0);
    }, 4000);
  });
});
