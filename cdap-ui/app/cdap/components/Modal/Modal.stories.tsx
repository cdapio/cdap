/*
 * Copyright © 2018 Cask Data, Inc.
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

import * as React from 'react';
import { storiesOf } from '@storybook/react';
import { action } from '@storybook/addon-actions';
import { withInfo } from '@storybook/addon-info';
import { withKnobs, boolean, text } from '@storybook/addon-knobs';
import { Modal, ModalHeader, ModalBody, ModalFooter } from 'reactstrap';
import IconSVG from '../IconSVG';
import Heading, { HeadingTypes } from 'components/Heading';
import ConfirmationModal from 'components/ConfirmationModal';

storiesOf('Modals', module)
  .addDecorator(withKnobs)
  .add(
    'Default Modal',
    withInfo({
      text: 'Simple modal with no customization',
    })(() => (
      <React.Fragment>
        <Heading type={HeadingTypes.h2} label="Default Modal component" />
        <Modal
          isOpen={boolean('Open Modal', true)}
          toggle={action('modal toggled')}
          className="cdap-modal"
          backdrop={boolean('Modal backdrop', true)}
          zIndex={1061}
          keyboard={text('Modal Keyboard', true)}
        >
          <ModalHeader>
            {text('Modal title', 'Default Modal')}
            <div className="close-section float-xs-right" onClick={action('modal closed')}>
              <IconSVG name="icon-close" />
            </div>
          </ModalHeader>
          <ModalBody>
            <div>
              {text('Modal content', 'Default modal content. By default we have some text')}
            </div>
          </ModalBody>
          <ModalFooter>
            <button className="btn btn-primary" onClick={action('modal ok button clicked')}>
              {text('Ok Button text', 'Ok Button')}
            </button>
            <button className="btn btn-secondary" onClick={action('modal cancel button clicked')}>
              {text('Cancel Button text', 'Cancel Button')}
            </button>
          </ModalFooter>
        </Modal>
      </React.Fragment>
    ))
  )
  .add(
    'Confirmation Modal',
    withInfo({
      text: 'Confirmation modal specifically used for user confirmation',
    })(() => (
      <React.Fragment>
        <Heading type={HeadingTypes.h2} label="Confirmation Modal component" />
        <ConfirmationModal
          cancelButtonText={text('Cancel Button text', 'Cancel')}
          cancelFn={action('Cancel Button clicked')}
          confirmButtonText={text('Confirm Button text', 'Ok')}
          confirmationText={text(
            'Confirmation message',
            'Could you please verify before performing this action?'
          )}
          confirmFn={action('Confirm Button clicked')}
          headerTitle={text('Header title', 'Are you sure?')}
          isOpen={boolean('Open Modal', true)}
          toggleModal={action('Toggle confirm modal')}
          isLoading={boolean('Set loading in modal', false)}
          errorMessage={text('Modal Error Message', '')}
          extendedMessage={text('Modal extended error message', '')}
          disableAction={boolean('Disable all actions', false)}
          closeable={boolean('Enable modal to be closed', true)}
          keyboard={boolean('Enable keyboard interactions', true)}
        />
      </React.Fragment>
    ))
  );
