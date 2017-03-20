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

import WizardModal from 'components/WizardModal';

describe('MarketplaceEntity Unit tests', () => {
  let wizardModal;
  const toggle = jest.fn();
  const modalContent = React.createElement('div', {className: 'modal-content'});
  beforeEach(() => {
    wizardModal = mount(
      <WizardModal
        title='Test Title'
        className="custom-classname"
        isOpen={true}
        toggle={toggle}
        children={modalContent}
      />
    );
  });
  afterEach(() => {
    wizardModal.unmount();
  });
  it('Should render', () => {
    expect(document.getElementsByClassName('wizard-modal').length).toBe(1);
  });
  it('Should render with the provided class name', () => {
    expect(document.getElementsByClassName('custom-classname').length).toBe(1);
  });
  it('Should render with the provided title', () => {
    expect(document.querySelector('.wizard-modal .modal-title span').innerHTML).toBe('Test Title');
  });
  it('Should be opened or not opened depending on the value of isOpen', () => {
    let wizardModal2 = mount(
      <WizardModal
        title='Test Title'
        isOpen={false}
        toggle={() => {}}
        className="custom-classname2"
        children={modalContent}
      />
    );
    expect(document.getElementsByClassName('custom-classname').length).toBe(1);
    expect(document.getElementsByClassName('custom-classname2').length).toBe(0);
    wizardModal2.unmount();
  });
  it('Should call toggle function when the bound element is clicked', () => {
    expect(toggle.mock.calls.length).toBe(0);
    let closeButtonContainer = document.querySelector('.close-section');
    closeButtonContainer.click();
    expect(toggle.mock.calls.length).toBe(1);
  });
  it('Should render the provided children elements', () => {
    expect(document.getElementById('modal-content')).toBeDefined();
    expect(document.getElementById('modal-contentz')).toBeNull();
  })
});
