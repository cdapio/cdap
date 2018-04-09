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
import VersionStore from 'services/VersionStore';
import VersionActions from 'services/VersionStore/VersionActions';
import MyCDAPVersionApi from 'api/version';
import {MyServiceProviderApi} from 'api/serviceproviders';
import {humanReadableDuration, humanReadableNumber, HUMANREADABLESTORAGE} from 'services/helpers';
import isNil from 'lodash/isNil';
import classnames from 'classnames';
import AdminManagementTabContent from 'components/Administration/AdminManagementTabContent';
import AdminConfigTabContent from 'components/Administration/AdminConfigTabContent';
import T from 'i18n-react';
import {objectQuery} from 'services/helpers';

require('./Administration.scss');

const PREFIX = 'features.Administration';
const WAITTIME_FOR_ALTERNATE_STATUS = 10000;

const ADMIN_TABS = {
  management: T.translate(`${PREFIX}.Tabs.management`),
  config: T.translate(`${PREFIX}.Tabs.config`)
};

class Administration extends Component {
  state = {
    platformsDetails: {},
    platforms: [],
    uptime: 0,
    loading: true,
    currentTab: objectQuery(this.props.location, 'state', 'showConfigTab') ? ADMIN_TABS.config : ADMIN_TABS.management
  };

  static propTypes = {
    location: PropTypes.object
  }

  componentDidMount() {
    this.getPlatforms();
    if (!VersionStore.getState().version) {
      this.getCDAPVersion();
    } else {
      this.setState({ version : VersionStore.getState().version });
    }

    document.querySelector('#header-namespace-dropdown').style.display = 'none';
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

  getCDAPVersion() {
    MyCDAPVersionApi
      .get()
      .subscribe((res) => {
        this.setState({ version : res.version });
        VersionStore.dispatch({
          type: VersionActions.updateVersion,
          payload: {
            version: res.version
          }
        });
      });
  }

  toggleCurrentTab = (tab) => {
    this.setState({
      currentTab: tab
    });
  };

  renderTabTitle() {
    return (
      <span className="tab-title">
        <h5
          className={classnames({"active": this.state.currentTab === ADMIN_TABS.management})}
          onClick={this.toggleCurrentTab.bind(this, ADMIN_TABS.management)}
        >
          {T.translate(`${PREFIX}.Tabs.management`)}
        </h5>
        <span className="divider"> | </span>
        <h5
          className={classnames({"active": this.state.currentTab === ADMIN_TABS.config})}
          onClick={this.toggleCurrentTab.bind(this, ADMIN_TABS.config)}
        >
          {T.translate(`${PREFIX}.Tabs.config`)}
        </h5>
      </span>
    );
  }

  renderUptimeVersion() {
    if (this.state.currentTab === ADMIN_TABS.config) {
      return null;
    }

    return (
      <span className="uptime-version-container">
        <span>
          {
            this.state.uptime ?
              T.translate(`${PREFIX}.uptimeLabel`, {
                time: humanReadableDuration(Math.ceil(this.state.uptime / 1000))
              })
            :
              null
          }
        </span>
        {
          isNil(this.state.version) ?
            null
          :
            (
              <i className="cdap-version">
                {T.translate(`${PREFIX}.Top.version-label`)} - {this.state.version}
              </i>
            )
        }
      </span>
    );
  }

  render () {
    return (
      <div className="administration">
        <Helmet
          title={T.translate(`${PREFIX}.TitleWithCDAP`)}
        />
        <div className="tab-title-and-version">
          {this.renderTabTitle()}
          {this.renderUptimeVersion()}
        </div>
        {
          this.state.currentTab === ADMIN_TABS.management ?
            <AdminManagementTabContent
              platformsDetails={this.state.platformsDetails}
              loading={this.state.loading}
            />
          :
            <AdminConfigTabContent />
        }
      </div>
    );
  }
}

export default Administration;
