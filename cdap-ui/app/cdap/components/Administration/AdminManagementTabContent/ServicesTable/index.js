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

import React, { Component } from 'react';
import SystemServicesStore from 'services/SystemServicesStore';
import isEqual from 'lodash/isEqual';
import SortableStickyTable from 'components/SortableStickyTable';
import T from 'i18n-react';
import IconSVG from 'components/IconSVG';
import classnames from 'classnames';
import LoadingSVG from 'components/LoadingSVG';
import { MyServiceProviderApi } from 'api/serviceproviders';
import Alert from 'components/Alert';
import If from 'components/If';
import { Theme } from 'services/ThemeHelper';
import moment from 'moment';
import { MyAppApi } from 'api/app';
import { Observable } from 'rxjs/Observable';
import sortBy from 'lodash/sortBy';
import startCase from 'lodash/startCase';
import { convertProgramToApi } from 'services/program-api-converter';

require('./ServicesTable.scss');

const WAITTIME_FOR_ALTERNATE_STATUS = 10000;
const ADMINPREFIX = 'features.Administration.Services';
const DEFAULTSERVICES = [
  'appfabric',
  'dataset.executor',
  'explore.service',
  'log.saver',
  'messaging.service',
  'metadata.service',
  'metrics',
  'metrics.processor',
  'transaction',
  'runtime',
];
const tableHeaders = [
  {
    label: T.translate(`${ADMINPREFIX}.headers.status`),
    property: 'status',
  },
  {
    label: T.translate(`${ADMINPREFIX}.headers.name`),
    property: 'name',
    defaultSortby: true,
  },
  {
    label: T.translate(`${ADMINPREFIX}.headers.provisioned`),
    property: 'provisioned',
  },
  {
    label: T.translate(`${ADMINPREFIX}.headers.requested`),
    property: 'requested',
  },
  {
    label: '',
    property: '',
  },
];

if (Theme.showSystemServicesInstance === false) {
  tableHeaders.splice(2, 2);
}
export default class ServicesTable extends Component {
  state = {
    services: SystemServicesStore.getState().services.list,
    systemProgramsStatus: [],
    showAlert: false,
    alertType: null,
    alertMessage: null,
  };

  servicePolls = [];

  resetAlert = () => {
    this.setState({
      showAlert: false,
      alertType: null,
      alertMessage: null,
    });
  };

  /*
    - Make call to /system/services
    - If it doesn't return within 10 seconds
      |  - Call individual services. /system/services/:serviceid
         |- If THAT didn't return within 10 seconds say the service is NOTOK
         |- If that returns render the service status
    - If it returns everything is just normal

    The same goes for instances too. Except UI won't make any assumptions on instances if it never returns.
  */

  fetchServiceStatus = (serviceid) => {
    const setDefaultStatus = (serviceid) => {
      let services = [...this.state.services];
      let isServiceAlreadyExist = this.state.services.find((service) => service.name === serviceid);
      if (!isServiceAlreadyExist) {
        services.push({
          name: serviceid,
          status: 'NOTOK',
        });
      } else {
        services = services.map((service) => {
          if (service.name === serviceid) {
            service.status = 'NOTOK';
          }
          return service;
        });
      }
      this.setState({ services });
    };

    const setDefaultInstance = (serviceid, { requested = '--', provisioned = '--' } = {}) => {
      let services = [...this.state.services];
      let isServiceAlreadyExist = this.state.services.find((service) => service.name === serviceid);
      if (!isServiceAlreadyExist) {
        services.push({
          name: serviceid,
          requested,
          provisioned,
        });
      } else {
        services = services.map((service) => {
          if (service.name === serviceid) {
            service.requested = requested;
            service.provisioned = provisioned;
          }
          return service;
        });
      }
      this.setState({ services });
    };

    let serviceTimeout = setTimeout(
      () => setDefaultStatus(serviceid),
      WAITTIME_FOR_ALTERNATE_STATUS
    );

    this.servicePolls.forEach((servicePoll) => servicePoll.unsubscribe());
    this.servicePolls.push(
      MyServiceProviderApi.pollServiceStatus({ serviceid }).subscribe(
        (res) => {
          clearTimeout(serviceTimeout);
          let services = [...this.state.services];
          services = services.map((service) => {
            if (service.name == serviceid) {
              service.status = res.staus;
            }
            return service;
          });
          this.setState({ services });
        },
        () => {
          setDefaultStatus(serviceid);
        }
      )
    );
    MyServiceProviderApi.getInstances({ serviceid }).subscribe(
      (res) => {
        setDefaultInstance(serviceid, res);
      },
      () => {
        setDefaultInstance(serviceid);
      }
    );
  };

  // This is when backend does not return for /system/services call
  // Make calls to individual services to get their status
  fetchStatusFromIndividualServices = () => {
    DEFAULTSERVICES.forEach((service) => this.fetchServiceStatus(service));
  };

  fetchSystemApps = () => {
    const programs = [];
    MyAppApi.list({ namespace: 'system' }).subscribe((apps) => {
      const requestPrograms = [];
      apps.forEach((app) => {
        requestPrograms.push(MyAppApi.get({ namespace: 'system', appId: app.name }));
      });

      Observable.combineLatest(requestPrograms).subscribe((res) => {
        res.forEach((appResponse) => {
          appResponse.programs.forEach((program) => {
            programs.push(program);
          });
        });

        this.fetchSystemProgramsStatus(programs);
      });
    });
  };

  fetchSystemProgramsStatus = (programs) => {
    const requestBody = programs.map((program) => {
      return {
        appId: program.app,
        programType: program.type,
        programId: program.name,
      };
    });

    MyAppApi.batchStatus({ namespace: 'system' }, requestBody).subscribe((res) => {
      const statuses = [];
      // CDAP-15254: We need to make this rename until backend changes
      // the name of the app from dataprep to wrangler
      const getAppId = (appId) => (appId === 'dataprep' ? 'wrangler' : appId);

      res.forEach((program) => {
        const systemProgram = {
          name: `${getAppId(program.appId)}.${program.programId}`,
          provisioned: '--',
          requested: '--',
          status: program.status,
          isSystemProgram: true,
          programId: program.programId,
          programType: program.programType,
          appId: program.appId,
        };

        statuses.push(systemProgram);
      });

      this.setState({
        systemProgramsStatus: statuses,
      });
    });
  };

  componentDidMount() {
    this.systemServicesSubscription = SystemServicesStore.subscribe(() => {
      let { list: services, __error } = SystemServicesStore.getState().services;
      if (__error) {
        this.fetchStatusFromIndividualServices();
        return;
      }
      if (!isEqual(services, this.state.services)) {
        this.setState({
          services,
        });
        this.servicePolls.forEach((servicePoll) => servicePoll.unsubscribe());
      }
    });

    this.fetchSystemApps();
  }

  componentWillUnmount() {
    if (this.systemServicesSubscription) {
      this.systemServicesSubscription();
    }
  }

  renderTableBody = (services) => {
    const start = moment()
      .subtract(7, 'days')
      .format('X');

    return (
      <table className="table-sm">
        <tbody>
          {services.map((service) => {
            let logUrl = `/v3/system/services/${service.name}/logs`;
            if (service.isSystemProgram) {
              logUrl = `/v3/namespaces/system/apps/${service.appId}/${convertProgramToApi(
                service.programType
              )}/${service.programId}/logs`;
            }

            logUrl = `${logUrl}?start=${start}`;
            logUrl = `/downloadLogs?type=raw&backendPath=${encodeURIComponent(logUrl)}`;

            const displayName = service.isSystemProgram
              ? startCase(service.name)
              : T.translate(`${ADMINPREFIX}.${service.name.replace(/\./g, '_')}`);

            return (
              <tr key={service.name}>
                <td>
                  <span className="status-circle">
                    <IconSVG
                      name="icon-circle"
                      className={classnames({
                        'text-success': ['OK', 'RUNNING'].indexOf(service.status) !== -1,
                        'text-warning': service.status === 'STARTING',
                        'text-danger':
                          ['OK', 'NOTOK', 'RUNNING', 'STARTING'].indexOf(service.status) === -1,
                      })}
                    />
                  </span>
                </td>
                <td>
                  <span>{displayName}</span>
                </td>
                <If condition={Theme.showSystemServicesInstance !== false}>
                  <td>
                    <span>{service.provisioned || '--'}</span>
                  </td>
                  <td>
                    <span className="requested-instances-holder">{service.requested || '--'}</span>
                  </td>
                </If>
                <td>
                  <a href={logUrl} target="_blank" rel="noopener noreferrer">
                    {T.translate(`${ADMINPREFIX}.viewlogs`)}
                  </a>
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    );
  };

  render() {
    if (!Object.keys(this.state.services).length) {
      return (
        <div className="services-table">
          <LoadingSVG />
        </div>
      );
    }

    const combinedSystemServices = sortBy(
      this.state.services.concat(this.state.systemProgramsStatus),
      ['name']
    );

    return (
      <div className="services-table">
        <SortableStickyTable
          className="table-sm"
          entities={combinedSystemServices}
          tableHeaders={tableHeaders}
          renderTableBody={this.renderTableBody}
        />
        <Alert
          showAlert={this.state.showAlert}
          type={this.state.alertType}
          message={this.state.alertMessage}
          onClose={this.resetAlert}
        />
      </div>
    );
  }
}
