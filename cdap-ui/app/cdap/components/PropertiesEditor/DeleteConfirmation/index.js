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

import React, { Component, PropTypes } from 'react';
import ConfirmationModal from 'components/ConfirmationModal';
import {MyMetadataApi} from 'api/metadata';
import NamespaceStore from 'services/NamespaceStore';
import T from 'i18n-react';

export default class DeleteConfirmation extends Component {
  constructor(props) {
    super(props);

    this.state = {
      isOpen: false,
      error: null,
      loading: false
    };

    this.toggleModal = this.toggleModal.bind(this);
    this.deleteProperty = this.deleteProperty.bind(this);
  }

  toggleModal() {
    this.setState({isOpen: !this.state.isOpen});
  }

  deleteProperty() {
    this.setState({loading: true});
    let namespace = NamespaceStore.getState().selectedNamespace;
    const params = {
      namespace,
      key: this.props.property.key,
      entityType: this.props.entityType,
      entityId: this.props.entityId,
    };

    MyMetadataApi.deleteProperty(params)
      .subscribe(() => {
        this.setState({
          isOpen: false,
          error: null,
          loading: false
        });
        this.props.onDelete();
      }, (err) => {
        this.setState({
          error: err,
          loading: false
        });
      });
  }

  render() {
    let error;
    if (this.state.error) {
      error = T.translate('features.PropertiesEditor.DeleteConfirmation.shortError');
    }

    return (
      <span>
        <span
          className="fa fa-trash"
          onClick={this.toggleModal}
        />

        <ConfirmationModal
          headerTitle={T.translate('features.PropertiesEditor.DeleteConfirmation.headerTitle')}
          toggleModal={this.toggleModal}
          confirmationText={T.translate('features.PropertiesEditor.DeleteConfirmation.confirmationText', {key: this.props.property.key})}
          confirmButtonText={T.translate('features.PropertiesEditor.DeleteConfirmation.confirmButton')}
          confirmFn={this.deleteProperty}
          cancelFn={this.toggleModal}
          isOpen={this.state.isOpen}
          isLoading={this.state.loading}
          errorMessage={error}
          extendedMessage={this.state.error}
        />
      </span>
    );
  }
}

DeleteConfirmation.propTypes = {
  property: PropTypes.object,
  onDelete: PropTypes.func,
  entityType: PropTypes.string,
  entityId: PropTypes.string
};
