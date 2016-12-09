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

import React, { Component, PropTypes } from 'react';
import { Modal, ModalHeader, ModalBody, ModalFooter } from 'reactstrap';
import CardActionFeedback from 'components/CardActionFeedback';
import Mousetrap from 'mousetrap';
import T from 'i18n-react';

require('./ConfirmationModal.less');

export default class ConfirmationModal extends Component {
  constructor(props) {
    super(props);
  }

  componentWillMount() {
    Mousetrap.bind('enter', this.props.confirmFn);
  }

  componentWillUnmount() {
    Mousetrap.unbind('enter');
  }

  renderModalBody() {
    if (this.props.isLoading) {
      return (
        <ModalBody className="loading">
          <h3 className="text-center">
            <span className="fa fa-spinner fa-spin"></span>
          </h3>
        </ModalBody>
      );
    }

    let confirmation = this.props.confirmationElem ? this.props.confirmationElem : this.props.confirmationText;

    let actionBtn;

    if(this.props.disableAction){
      actionBtn = (
        <button
          className="btn disabled-btn"
          disabled
        >
          {this.props.confirmButtonText}
        </button>
      );
    } else {
      actionBtn = (
        <button
          className="btn"
          onClick={this.props.confirmFn}
        >
          {this.props.confirmButtonText}
        </button>
      );
    }

    return (
      <ModalBody>
        <div className="confirmation">
          {confirmation}
        </div>
        <div className="confirmation-button-options">
          {actionBtn}
          <button
            className="btn"
            onClick={this.props.cancelFn}
          >
            {this.props.cancelButtonText}
          </button>
        </div>
      </ModalBody>
    );
  }

  render() {
    let footer;
    if (this.props.errorMessage) {
      footer = (
        <ModalFooter>
          <CardActionFeedback
            type="DANGER"
            message={this.props.errorMessage}
            extendedMessage={this.props.extendedMessage}
          />
        </ModalFooter>
      );
    }

    return(
      <Modal
        isOpen={this.props.isOpen}
        toggle={this.props.toggleModal}
        className="confirmation-modal"
      >
        <ModalHeader>
          {this.props.headerTitle}
        </ModalHeader>

        {this.renderModalBody()}
        {footer}
      </Modal>
    );
  }
}

ConfirmationModal.defaultProps = {
  confirmButtonText: T.translate('features.ConfirmationModal.confirmDefaultText'),
  cancelButtonText:  T.translate('features.ConfirmationModal.cancelDefaultText')
};

ConfirmationModal.propTypes = {
  cancelButtonText: PropTypes.string,
  cancelFn: PropTypes.func,
  confirmationElem: PropTypes.element,
  confirmButtonText: PropTypes.string,
  confirmationText: PropTypes.oneOfType([
    PropTypes.string,
    PropTypes.arrayOf(PropTypes.oneOfType([
      PropTypes.string,
      PropTypes.node
    ]))
  ]),
  confirmFn: PropTypes.func,
  headerTitle: PropTypes.string,
  isOpen: PropTypes.bool,
  toggleModal: PropTypes.func,
  isLoading: PropTypes.bool,
  errorMessage: PropTypes.string,
  extendedMessage: PropTypes.string,
  disableAction: PropTypes.bool
};
