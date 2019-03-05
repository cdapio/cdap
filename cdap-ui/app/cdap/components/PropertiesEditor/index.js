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
import { MyMetadataApi } from 'api/metadata';
import { getCurrentNamespace } from 'services/NamespaceStore';
import uuidV4 from 'uuid/v4';
import AddPropertyModal from 'components/PropertiesEditor/AddPropertyModal';
import T from 'i18n-react';
import DeleteConfirmation from 'components/PropertiesEditor/DeleteConfirmation';
import EditProperty from 'components/PropertiesEditor/EditProperty';
import classnames from 'classnames';
import { SCOPES } from 'services/global-constants';

require('./PropertiesEditor.scss');

export default class PropertiesEditor extends Component {
  constructor(props) {
    super(props);

    this.state = {
      systemProperties: [],
      userProperties: [],
      activeEdit: null,
      newValue: '',
      editedKey: null,
    };
  }

  componentWillMount() {
    this.fetchProperties();
  }

  fetchProperties = () => {
    const params = {
      namespace: getCurrentNamespace(),
      entityType: this.props.entityType,
      entityId: this.props.entityId,
    };

    MyMetadataApi.getProperties(params)
      .map((res) => {
        return res.properties.map((property) => {
          property.id = uuidV4();
          return property;
        });
      })
      .subscribe((res) => {
        this.setState({
          systemProperties: res.filter((property) => property.scope === SCOPES.SYSTEM),
          userProperties: res.filter((property) => property.scope === SCOPES.USER),
          activeEdit: null,
          newValue: '',
        });
      });
  };

  renderSystemProperties() {
    return this.state.systemProperties.map((row) => {
      return (
        <tr key={row.id}>
          <td>{row.name}</td>
          <td>{row.value}</td>
          <td>{T.translate('features.PropertiesEditor.system')}</td>
          <td />
        </tr>
      );
    });
  }

  renderActions(row) {
    return (
      <span>
        <EditProperty
          property={row}
          onSave={this.setEditProperty.bind(this, row)}
          entityType={this.props.entityType}
          entityId={this.props.entityId}
        />

        <DeleteConfirmation
          property={row}
          onDelete={this.fetchProperties}
          entityType={this.props.entityType}
          entityId={this.props.entityId}
        />
      </span>
    );
  }

  renderUserProperties() {
    return this.state.userProperties.map((row) => {
      return (
        <tr
          key={row.id}
          className={classnames({ 'text-success': row.name === this.state.editedKey })}
        >
          <td>{row.name}</td>
          <td>{row.value}</td>
          <td>{T.translate('features.PropertiesEditor.user')}</td>
          <td className="actions">{this.renderActions(row)}</td>
        </tr>
      );
    });
  }

  setEditProperty(row) {
    this.setState({ editedKey: row.name });

    this.fetchProperties();

    setTimeout(() => {
      this.setState({ editedKey: null });
    }, 3000);
  }

  render() {
    return (
      <div className="properties-editor-container">
        <AddPropertyModal
          entityId={this.props.entityId}
          entityType={this.props.entityType}
          existingProperties={this.state.userProperties}
          onSave={this.setEditProperty.bind(this)}
        />

        <table className="table">
          <thead>
            <tr>
              <th className="key">{T.translate('features.PropertiesEditor.name')}</th>
              <th className="value">{T.translate('features.PropertiesEditor.value')}</th>
              <th className="scope">{T.translate('features.PropertiesEditor.scope')}</th>
              <th className="actions" />
            </tr>
          </thead>

          <tbody>
            {this.renderUserProperties()}
            {this.renderSystemProperties()}
          </tbody>
        </table>
      </div>
    );
  }
}

PropertiesEditor.propTypes = {
  entityId: PropTypes.string,
  entityType: PropTypes.oneOf(['datasets', 'apps']),
};
