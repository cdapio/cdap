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
import React, {Component, PropTypes} from 'react';
import NamespaceStore from 'services/NamespaceStore';
import SortableTable from 'components/SortableTable';
import {Link} from 'react-router';
import {convertEntityTypeToApi} from 'services/entity-type-api-converter';
import classnames from 'classnames';
import isNil from 'lodash/isNil';
import FastActions from 'components/EntityCard/FastActions';
import T from 'i18n-react';
require('./DatasetStreamTable.scss');

export default class DatasetStreamTable extends Component {
  constructor(props) {
    super(props);

    this.state = {
      dataEntities: props.dataEntities
    };
    this.tableHeaders = [
      {
        property: 'name',
        label: T.translate('features.ViewSwitch.nameLabel')
      },
      {
        property: 'type',
        label: T.translate('features.ViewSwitch.typeLabel')
      },
      {
        label: T.translate('features.ViewSwitch.DatasetStreamTable.readsLabel')
      },
      {
        label: T.translate('features.ViewSwitch.DatasetStreamTable.writesLabel')
      },
      {
        label: T.translate('features.ViewSwitch.DatasetStreamTable.eventsLabel')
      },
      {
        label: T.translate('features.ViewSwitch.DatasetStreamTable.sizeLabel')
      },
      {
        label: ''
      }
    ];
  }

  componentWillMount() {
    this.setState({
      dataEntities: this.props.dataEntities
    });

  }

  componentWillReceiveProps(nextProps) {
    this.setState({
      dataEntities: nextProps.dataEntities
    });
  }

  renderTableBody(entities) {
    return (
      <tbody>
        {
          entities.map(dataEntity => {
            let currentNamespace = NamespaceStore.getState().selectedNamespace;
            let icon = dataEntity.type === 'datasetinstance' ? 'icon-datasets' : 'icon-streams';
            let type = dataEntity.type === 'datasetinstance' ? 'Dataset' : 'Stream';
            let link = `/ns/${currentNamespace}/${convertEntityTypeToApi(dataEntity.type)}/${dataEntity.id}`;
            return (
              // this is super ugly, but cannot wrap a link around a <tr> tag, so have to wrap it
              // around every <td>. Javascript solutions won't show the link when the user hovers
              // over the element.
              <tr key={dataEntity.uniqueId}>
                <td>
                  <Link
                    to={link}
                     title={dataEntity.name}
                  >
                    {dataEntity.name}
                  </Link>
                </td>
                <td>
                  <Link to={link}>
                    <i className={classnames('fa', icon)}></i>
                    <span>{type}</span>
                  </Link>
                </td>
                <td>
                  <Link to={link}>{dataEntity.reads}</Link>
                </td>
                <td>
                  <Link to={link}>{dataEntity.writes}</Link>
                </td>
                <td>
                  <Link to={link}>{dataEntity.events}</Link>
                </td>
                <td>
                  <Link to={link}>{dataEntity.bytes}</Link>
                </td>
                <td>
                  <Link to={link}>
                    <div className="fast-actions-container text-xs-center">
                      <FastActions
                        className="text-xs-left btn-group"
                        entity={dataEntity}
                      />
                    </div>
                  </Link>
                </td>
              </tr>
            );
          })
        }
      </tbody>
    );
  }

  render() {
    // we don't want to load until we have all these info
    const isAllLoaded = () => {
      return this.props.dataEntities.every(dataEntity => {
        return !isNil(dataEntity.reads) && !isNil(dataEntity.writes) && !isNil(dataEntity.events) && !isNil(dataEntity.bytes);
      });
    };
    if (!isAllLoaded()) {
      return (
        <div className="dataentity-table">
          <h3 className="text-xs-center">
            <span className="fa fa-spinner fa-spin fa-2x loading-spinner"></span>
          </h3>
        </div>
      );
    }
    return (
      <div className="dataentity-table">
        <SortableTable
          className="table-sm"
          entities={this.state.dataEntities}
          tableHeaders={this.tableHeaders}
          renderTableBody={this.renderTableBody.bind(this)}
        />
      </div>
    );
  }
}
DatasetStreamTable.propTypes = {
  dataEntities: PropTypes.arrayOf(PropTypes.object)
};
