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
import React, {PropTypes} from 'react';
import classnames from 'classnames';
import FastActions from 'components/EntityCard/FastActions';
require('./DatasetStreamTable.scss');
import T from 'i18n-react';
export default function DatasetStreamTable({dataEntities}) {
  return (
    <div className="dataentity-table">
      <table className="table table-bordered table-sm">
        <thead>
          <tr>
            <th>{T.translate('features.ViewSwitch.nameLabel')}</th>
            <th>{T.translate('features.ViewSwitch.typeLabel')}</th>
            <th>{T.translate('features.ViewSwitch.DatasetStreamTable.readsLabel')}</th>
            <th>{T.translate('features.ViewSwitch.DatasetStreamTable.writesLabel')}</th>
            <th>{T.translate('features.ViewSwitch.DatasetStreamTable.eventsLabel')}</th>
            <th>{T.translate('features.ViewSwitch.DatasetStreamTable.sizeLabel')}</th>
            <th>{T.translate('features.ViewSwitch.actionsLabel')}</th>
          </tr>
        </thead>
        <tbody>
          {
            dataEntities.map(dataEntity => {
              let icon = dataEntity.type === 'datasetinstance' ? 'icon-datasets' : 'icon-streams';
              let type = dataEntity.type === 'datasetinstance' ? 'Dataset' : 'Stream';
              return (
                <tr key={dataEntity.uniqueId}>
                  <td>{dataEntity.name}</td>
                  <td>
                    <i className={classnames('fa', icon)}></i>
                    <span>{type}</span>
                  </td>
                  <td>{dataEntity.reads}</td>
                  <td>{dataEntity.writes}</td>
                  <td>{dataEntity.events}</td>
                  <td>{dataEntity.bytes}</td>
                  <td>
                    <div className="fast-actions-container">
                      <FastActions
                        className="text-xs-left"
                        entity={dataEntity}
                      />
                    </div>
                  </td>
                </tr>
              );
            })
          }
        </tbody>
      </table>
    </div>
  );
}
DatasetStreamTable.propTypes = {
  dataEntities: PropTypes.arrayOf(PropTypes.object)
};
