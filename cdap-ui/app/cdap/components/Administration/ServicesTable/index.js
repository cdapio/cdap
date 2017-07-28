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

import React, {Component} from 'react';
import SystemServicesStore from 'services/SystemServicesStore';
import isEqual from 'lodash/isEqual';
import SortableStickyTable from 'components/SortableStickyTable';
import T from 'i18n-react';
import IconSVG from 'components/IconSVG';
import classnames from 'classnames';
import Datasource from 'services/datasource';
import LoadingSVG from 'components/LoadingSVG';

require('./ServicesTable.scss');

const ADMINPREFIX = 'features.Administration.Services';
const tableHeaders = [
  {
    label: T.translate(`${ADMINPREFIX}.headers.status`),
    property: 'status'
  },
  {
    label: T.translate(`${ADMINPREFIX}.headers.name`),
    property: 'name',
    defaultSortby: true
  },
  {
    label: T.translate(`${ADMINPREFIX}.headers.provisioned`),
    property: 'provisioned'
  },
  {
    label: T.translate(`${ADMINPREFIX}.headers.requested`),
    property: 'requested'
  },
  {
    label: '',
    property: ''
  }
];
export default class ServicesTable extends Component {
  state = {
    services: SystemServicesStore.getState().services
  }

  componentDidMount() {
    this.systemServicesSubscription = SystemServicesStore.subscribe(() => {
      let {services} = SystemServicesStore.getState();
      if (!isEqual(services, this.state.services)) {
        this.setState({
          services
        });
      }
    });
  }

  componentWillUnmount() {
    if (this.systemServicesSubscription) {
      this.systemServicesSubscription();
    }
  }

  renderTableBody = (services) => {
    return (
      <table className="table-sm">
        <tbody>
          {
            services.map(service => {
               let logUrl = Datasource.constructUrl({
                _cdapPath : `/system/services/${service.name}/logs`
              });

              logUrl = `/downloadLogs?type=raw&backendUrl=${encodeURIComponent(logUrl)}`;

              return (
                <tr>
                  <td>
                    <span className="status-circle">
                      <IconSVG
                        name="icon-circle"
                        className={classnames({
                          "text-success": service.status === 'OK',
                          "text-danger": service.status === 'NOTOK'
                        })}
                      />
                    </span>
                  </td>
                  <td>
                    <span>{T.translate(`${ADMINPREFIX}.${service.name.replace(/\./g, '_')}`)}</span>
                  </td>
                  <td>
                    <span>{service.provisioned}</span>
                  </td>
                  <td>
                    <span>{service.requested}</span>
                  </td>
                  <td>
                    <a href={logUrl} target="_blank">{T.translate(`${ADMINPREFIX}.viewlogs`)}</a></td>
                </tr>
              );
            })
          }
        </tbody>
      </table>
    );
  };

  render() {
    if (!this.state.services.length) {
      return (
        <div className="services-table">
          <LoadingSVG />
        </div>
      );
    }
    return (
      <div className="services-table">
        <SortableStickyTable
          className="table-sm"
          entities={this.state.services}
          tableHeaders={tableHeaders}
          renderTableBody={this.renderTableBody}
        />
      </div>
    );
  }
}

