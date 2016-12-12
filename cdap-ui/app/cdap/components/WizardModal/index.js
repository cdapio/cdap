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
import React, {PropTypes} from 'react';
import {Modal, ModalBody, ModalHeader} from 'reactstrap';
import ReactCSSTransitionGroup from 'react-addons-css-transition-group';
import classnames from 'classnames';

require('./WizardModal.less');

export default function WizardModal({children, title, isOpen, toggle, className}) {
  return (
    <Modal
      isOpen={isOpen}
      toggle={toggle}
      className={classnames("wizard-modal", className)}
      size="lg"
    >
      <ModalHeader>
        <span className="pull-left">
          {title}
        </span>
        <div
          className="close-section pull-right"
          onClick={toggle}
        >
          <span
            className="fa fa-times"
          >
          </span>
        </div>
      </ModalHeader>
      <ModalBody>
        <ReactCSSTransitionGroup
          component="div"
          transitionName="wizard-modal-content"
          transitionEnterTimeout={500}
          transitionLeaveTimeout={300}
        >
          {children}
        </ReactCSSTransitionGroup>
      </ModalBody>
    </Modal>
  );
}
WizardModal.defaultProps = {
  title: 'Wizard',
  isOpen: false,
  closeHandler: () => {},
  backdrop: false
};

WizardModal.propTypes = {
  children: PropTypes.node,
  title: PropTypes.string,
  isOpen: PropTypes.bool,
  toggle: PropTypes.func,
  className: PropTypes.string
};
