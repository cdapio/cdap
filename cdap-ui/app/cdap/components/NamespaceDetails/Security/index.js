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

import React, { Component } from 'react';
import { connect } from 'react-redux';
import PropTypes from 'prop-types';
import T from 'i18n-react';
import AddNamespaceWizard from 'components/CaskWizards/AddNamespace';
import { getNamespaceProperties } from 'components/NamespaceDetails/store/ActionCreator';
import { Theme } from 'services/ThemeHelper';
require('./Security.scss');

const PREFIX = 'features.NamespaceDetails.security';

const mapStateToProps = (state) => {
  return {
    name: state.name,
    description: state.description,
    namespacePrefs: state.namespacePrefs,
    hdfsRootDirectory: state.hdfsRootDirectory,
    hbaseNamespaceName: state.hbaseNamespaceName,
    hiveDatabaseName: state.hiveDatabaseName,
    schedulerQueueName: state.schedulerQueueName,
    principal: state.principal,
    keytabURI: state.keytabURI,
  };
};

class NamespaceDetailsSecurity extends Component {
  state = {
    modalOpen: false,
  };

  static propTypes = {
    name: PropTypes.string,
    description: PropTypes.string,
    namespacePrefs: PropTypes.object,
    hdfsRootDirectory: PropTypes.string,
    hbaseNamespaceName: PropTypes.string,
    hiveDatabaseName: PropTypes.string,
    schedulerQueueName: PropTypes.string,
    principal: PropTypes.string,
    keytabURI: PropTypes.string,
  };

  toggleModal = (namespaceEdited) => {
    if (namespaceEdited) {
      getNamespaceProperties();
    }
    this.setState({
      modalOpen: !this.state.modalOpen,
    });
  };

  render() {
    if (Theme.showNamespaceSecurity === false) {
      return null;
    }

    return (
      <React.Fragment>
        <hr />
        <div className="namespace-details-security">
          <div className="namespace-details-section-label">
            <strong>{T.translate(`${PREFIX}.label`)}</strong>
            {this.props.principal && this.props.keytabURI ? (
              <span className="edit-label" onClick={this.toggleModal}>
                {T.translate('features.NamespaceDetails.edit')}
              </span>
            ) : null}
          </div>
          <div className="security-values">
            <strong>{T.translate(`${PREFIX}.principal`)}</strong>
            <span title={this.props.principal}>{this.props.principal || '- -'}</span>
          </div>
          <div className="security-values">
            <strong>{T.translate(`${PREFIX}.keytabURI`)}</strong>
            <span title={this.props.keytabURI}>{this.props.keytabURI || '- -'}</span>
          </div>
          {this.state.modalOpen ? (
            <AddNamespaceWizard
              isOpen={this.state.modalOpen}
              onClose={this.toggleModal}
              isEdit={true}
              editableFields={['keyTab']}
              properties={{ ...this.props }}
              activeStepId="security"
            />
          ) : null}
        </div>
      </React.Fragment>
    );
  }
}

const ConnectedNamespaceDetailsSecurity = connect(mapStateToProps)(NamespaceDetailsSecurity);
export default ConnectedNamespaceDetailsSecurity;
