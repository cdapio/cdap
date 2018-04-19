/*
* Copyright © 2016-2018 Cask Data, Inc.
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
import PropTypes from 'prop-types';
import Helmet from 'react-helmet';
import {MyServiceProviderApi} from 'api/serviceproviders';
import {humanReadableNumber, HUMANREADABLESTORAGE} from 'services/helpers';
import AdminManagementTabContent from 'components/Administration/AdminManagementTabContent';
import AdminConfigTabContent from 'components/Administration/AdminConfigTabContent';
import AdminTabSwitch from 'components/Administration/AdminTabSwitch';
import T from 'i18n-react';
import {Route, Switch} from 'react-router-dom';

require('./Administration.scss');

const PREFIX = 'features.Administration';
const WAITTIME_FOR_ALTERNATE_STATUS = 10000;

class Administration extends Component {
  state = {
    platformsDetails: {},
    platforms: [],
    uptime: 0,
    loading: true,
    accordionToExpand: typeof this.props.location.state === 'object' ? this.props.location.state.accordionToExpand : null
  };

  static propTypes = {
    location: PropTypes.object
  }

  componentDidMount() {
    this.getPlatforms();
    document.querySelector('#header-namespace-dropdown').style.display = 'none';
  }

  componentWillReceiveProps(nextProps) {
    if (nextProps.location.state !== this.props.location.state) {
      let accordionToExpand = typeof nextProps.location.state === 'object' ? nextProps.location.state.accordionToExpand : null;
      this.setState({
        accordionToExpand
      });
    }
  }

  componentWillUnmount() {
    document.querySelector('#header-namespace-dropdown').style.display = 'inline-block';
  }

  getPlatforms() {
    MyServiceProviderApi
      .pollList()
      .subscribe(
        (res) => {
          let platformsDetails = {};
          let uptime;
          let platformNames = Object.keys(res);
          platformNames.map(key => {
            if (key !== 'cdap') {
              platformsDetails[key] = {
                name: T.translate(`features.Administration.Component-Overview.headers.${key}`),
                version: res[key].Version,
                url: res[key].WebURL,
                logs: res[key].LogsURL
              };
            } else {
              uptime = res[key].Uptime;
              platformsDetails[key] = {
                name: T.translate(`features.Administration.Component-Overview.headers.${key}`)
              };
            }
          });
          this.getPlatformDetails(platformNames);
          this.setState({
            platforms : platformNames,
            platformsDetails,
            loading: false,
            uptime
          });
        },
        () => {
          setTimeout(() => {
            this.getPlatforms();
          }, WAITTIME_FOR_ALTERNATE_STATUS);
        }
      );
  }

  prettifyPlatformDetails(name, details) {
    switch (name) {
      case 'hdfs':
        return Object.assign({}, details, {
          storage: {
            ...details.storage,
            UsedBytes: humanReadableNumber(details.storage.UsedBytes, HUMANREADABLESTORAGE),
            RemainingBytes: humanReadableNumber(details.storage.RemainingBytes, HUMANREADABLESTORAGE),
            TotalBytes: humanReadableNumber(details.storage.TotalBytes, HUMANREADABLESTORAGE),
          }
        });
      case 'cdap':
        delete details.transactions.WritePointer;
        delete details.transactions.ReadPointer;
        return Object.assign({}, details, {
          lasthourload: {
            ...details.lasthourload,
            Successful: humanReadableNumber(details.lasthourload.Successful),
            TotalRequests: humanReadableNumber(details.lasthourload.TotalRequests)
          }
        });
      case 'hbase':
        return Object.assign({}, details, {
          load: {
            ...details.load,
            TotalRegions: humanReadableNumber(details.load.TotalRegions),
            NumRequests: humanReadableNumber(details.load.NumRequests),
            AverageRegionsPerServer: humanReadableNumber(details.load.AverageRegionsPerServer)
          }
        });
      case 'yarn':
        return Object.assign({}, details, {
          resources: {
            ...details.resources,
            TotalMemory: humanReadableNumber(details.resources.TotalMemory, HUMANREADABLESTORAGE),
            FreeMemory: humanReadableNumber(details.resources.FreeMemory, HUMANREADABLESTORAGE),
            UsedMemory: humanReadableNumber(details.resources.UsedMemory, HUMANREADABLESTORAGE),
          }
        });
      default:
        return details;
    }
  }

  getPlatformDetails(platforms) {
    platforms.forEach((name) => {
      MyServiceProviderApi.get({
        serviceprovider : name
      })
      .subscribe( (res) => {
        let platformDetail = Object.assign({}, this.state.platformsDetails[name], res);
        let platformsDetails = Object.assign({}, this.state.platformsDetails, {
          [name]: this.prettifyPlatformDetails(name, platformDetail)
        });
        this.setState({ platformsDetails });
      });
    });
  }

  render () {
    return (
      <div className="administration">
        <Helmet
          title={T.translate(`${PREFIX}.TitleWithCDAP`)}
        />
        <AdminTabSwitch uptime={this.state.uptime} />
        <Switch>
          <Route exact path="/administration" render={() => {
            return (
              <AdminManagementTabContent
                platformsDetails={this.state.platformsDetails}
                loading={this.state.loading}
              />
            );
          }} />
          <Route exact path="/administration/configuration" render={() => {
            return (
              <AdminConfigTabContent
                accordionToExpand={this.state.accordionToExpand}
              />
            );
          }} />
        </Switch>
      </div>
    );
  }
}

export default Administration;
