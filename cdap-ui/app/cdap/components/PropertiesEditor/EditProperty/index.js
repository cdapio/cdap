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

import PropTypes from 'prop-types';

import React, { Component } from 'react';
import {Modal, ModalHeader, ModalBody, ModalFooter} from 'reactstrap';
import CardActionFeedback from 'components/CardActionFeedback';
import {MyMetadataApi} from 'api/metadata';
import NamespaceStore from 'services/NamespaceStore';
import T from 'i18n-react';

export default class EditProperty extends Component {
  constructor(props) {
    super(props);

    this.state = {
      isOpen: false,
      valueInput: '',
      error: null
    };

    this.toggleModal = this.toggleModal.bind(this);
    this.saveProperty = this.saveProperty.bind(this);
    this.handleValueChange = this.handleValueChange.bind(this);
  }

  componentDidUpdate() {
    if (this.state.isOpen && this.state.valueInput.length === 0) {
      document.getElementById('edit-property-modal-value-input').focus();
    }
  }

  toggleModal() {
    this.setState({isOpen: !this.state.isOpen});
  }

  saveProperty() {
    if (!this.state.valueInput) { return; }
    let namespace = NamespaceStore.getState().selectedNamespace;
    const params = {
      namespace,
      entityType: this.props.entityType,
      entityId: this.props.entityId,
      scope: 'USER'
    };

    let requestBody = {};
    requestBody[this.props.property.key] = this.state.valueInput;

    MyMetadataApi.addProperties(params, requestBody)
      .subscribe(() => {
        this.setState({
          isOpen: false,
          valueInput: ''
        });

        this.props.onSave();

      }, (err) => {
        this.setState({ error: err });
      });
  }

  handleValueChange(e) {
    this.setState({valueInput: e.target.value});
  }

  renderFeedback() {
    if (!this.state.error) { return null; }

    return (
      <ModalFooter>
        <CardActionFeedback
          type='DANGER'
          message={T.translate('features.PropertiesEditor.EditProperty.shortError')}
          extendedMessage={this.state.error}
        />
      </ModalFooter>
    );
  }

  renderModal() {
    if (!this.state.isOpen) { return null; }

    let disabled = this.state.valueInput.length === 0;

    return (
      <Modal
        isOpen={this.state.isOpen}
        toggle={this.toggleModal}
        size="md"
        className="add-property-modal"
      >
        <ModalHeader>
          <span>
            {T.translate('features.PropertiesEditor.EditProperty.modalHeader', {key: this.props.property.key})}
          </span>

          <div
            className="close-section float-xs-right"
            onClick={this.toggleModal}
          >
            <span className="fa fa-times" />
          </div>
        </ModalHeader>
        <ModalBody>
          <div className="row input-properties">
            <div className="col-xs-12">
              <input
                type="text"
                id="edit-property-modal-value-input"
                className="form-control"
                placeholder={T.translate('features.PropertiesEditor.EditProperty.valuePlaceholder')}
                value={this.state.valueInput}
                onChange={this.handleValueChange}
              />
            </div>
          </div>

          <div className="text-xs-right">
            <button
              className="btn btn-primary"
              onClick={this.saveProperty}
              disabled={disabled}
            >
              {T.translate('features.PropertiesEditor.EditProperty.button')}
            </button>
          </div>
        </ModalBody>

        {this.renderFeedback()}

      </Modal>
    );
  }

  render() {
    return (
      <span>
        <span
          className="fa fa-pencil"
          onClick={this.toggleModal}
        />

        {this.renderModal()}
      </span>
    );
  }
}

EditProperty.propTypes = {
  property: PropTypes.object,
  entityType: PropTypes.string,
  entityId: PropTypes.string,
  onSave: PropTypes.func
};
