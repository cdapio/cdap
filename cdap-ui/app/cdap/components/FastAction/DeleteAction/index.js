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

import React, {PropTypes, Component} from 'react';
import NamespaceStore from 'services/NamespaceStore';
import {MyAppApi} from 'api/app';
import {MyArtifactApi} from 'api/artifact';
import {MyDatasetApi} from 'api/dataset';
import {MyStreamApi} from 'api/stream';
import FastActionButton from '../FastActionButton';
import ConfirmationModal from 'components/ConfirmationModal';
import T from 'i18n-react';

export default class DeleteAction extends Component {
  constructor(props) {
    super(props);

    this.action = this.action.bind(this);
    this.toggleModal = this.toggleModal.bind(this);

    this.state = {
      modal: false,
      loading: false,
      errorMessage: '',
      extendedMessage: ''
    };
  }

  toggleModal() {
    this.setState({modal: !this.state.modal});
  }

  action() {
    this.setState({loading: true});
    let api;
    let params = {
      namespace: NamespaceStore.getState().selectedNamespace
    };
    switch (this.props.entity.type) {
      case 'application':
        api = MyAppApi.delete;
        params.appId = this.props.entity.id;
        break;
      case 'artifact':
        api = MyArtifactApi.delete;
        params.artifactId = this.props.entity.id;
        params.version = this.props.entity.version;
        break;
      case 'datasetinstance':
        api = MyDatasetApi.delete;
        params.datasetId = this.props.entity.id;
        break;
      case 'stream':
        api = MyStreamApi.delete;
        params.streamId = this.props.entity.id;
        break;
    }

    api(params)
      .subscribe(this.props.onSuccess, (err) => {
        this.setState({
          loading: false,
          errorMessage: T.translate('features.FastAction.deleteFailed', {entityId: this.props.entity.id}),
          extendedMessage: err
        });
      });
  }

  render() {
    return (
      <span>
        <FastActionButton
          icon="fa fa-trash"
          action={this.toggleModal}
        />
        {
          this.state.modal ? (
            <ConfirmationModal
              headerTitle={T.translate('features.FastAction.deleteTitle', {entityType: this.props.entity.type})}
              toggleModal={this.toggleModal}
              confirmationText={T.translate('features.FastAction.deleteConfirmation', {entityId: this.props.entity.id})}
              confirmButtonText={T.translate('features.FastAction.deleteConfirmButton')}
              confirmFn={this.action}
              cancelFn={this.toggleModal}
              isOpen={this.state.modal}
              isLoading={this.state.loading}
              errorMessage={this.state.errorMessage}
              extendedMessage={this.state.extendedMessage}
            />
          ) : null
        }
      </span>
    );
  }
}

DeleteAction.propTypes = {
  entity: PropTypes.shape({
    id: PropTypes.string.isRequired,
    version: PropTypes.string,
    type: PropTypes.oneOf(['application', 'artifact', 'datasetinstance', 'stream']).isRequired
  }),
  onSuccess: PropTypes.func
};
