/*
* Copyright © 2016-2017 Cask Data, Inc.
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
require('./Administration.scss');
import AdminConfigurePane from '../AdminConfigurePane';
import AdminOverviewPane from '../AdminOverviewPane';
import AbstractWizard from '../AbstractWizard';
import SetPreferenceModal from '../FastAction/SetPreferenceAction/SetPreferenceModal';
import Helmet from 'react-helmet';
import VersionStore from 'services/VersionStore';
import VersionActions from 'services/VersionStore/VersionActions';
import MyCDAPVersionApi from 'api/version';
import {MyServiceProviderApi} from 'api/serviceproviders';
import {humanReadableDuration} from 'services/helpers';
import T from 'i18n-react';
import PlatformsDetails from 'components/Administration/PlatformsDetails';
import ServicesTable from 'components/Administration/ServicesTable';
import {humanReadableNumber, HUMANREADABLESTORAGE} from 'services/helpers';
import isNil from 'lodash/isNil';

const PREFIX = 'features.Administration';
const WAITTIME_FOR_ALTERNATE_STATUS = 10000;

class Administration extends Component {

  state = {
    services: [],
    platformsDetails: {},
    platforms: [],
    activeApplication: null,
    wizard : {
      actionIndex : null,
      actionType : null
    },
    preferenceModal: false,
    uptime: 0,
    showNamespaceWizard: false,
    loading: true
  };

  componentWillUnmount() {
    document.querySelector('#header-namespace-dropdown').style.display = 'inline-block';
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

  onSystemPreferencesSaved = () => {
    this.setState({
      preferencesSaved: true
    });
    setTimeout(() => {
      this.setState({
        preferencesSaved: false
      });
    }, 3000);
  };

  closePreferencesSavedMessage = () => {
    this.setState({
      preferencesSaved: false
    });
  };

  closeWizard = () => {
    this.setState({
      showNamespaceWizard: false
    });
  };

  openNamespaceWizard = () => {
    this.setState({
      showNamespaceWizard: true
    });
  };

  togglePreferenceModal = () => {
    this.setState({
      preferenceModal: !this.state.preferenceModal
    });
  };

  render () {
    return (
       <div className="administration">
        <Helmet
          title={T.translate(`${PREFIX}.Title`)}
        />
        <div>
          <div className="page-title-and-version">
            <span>
              <h3>{T.translate(`${PREFIX}.Title`)}</h3>
            </span>
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
          </div>
        </div>
        <div className="services-details">
          <div className="services-table-section">
            <strong> {T.translate(`${PREFIX}.Services.title`)} </strong>
            <ServicesTable />
          </div>
          <div className="platform-section">
            <PlatformsDetails platforms={this.state.platformsDetails} />
          </div>
        </div>
        <div className="admin-bottom-panel">
          <AdminConfigurePane
            openNamespaceWizard={this.openNamespaceWizard}
            openPreferenceModal={this.togglePreferenceModal}
            preferencesSavedState={this.state.preferencesSaved}
            closePreferencesSavedMessage={this.closePreferencesSavedMessage}
          />
          <AdminOverviewPane
            isLoading={this.state.loading}
            platforms={this.state.platformsDetails}
          />
        </div>
        <AbstractWizard
          isOpen={this.state.showNamespaceWizard}
          onClose={this.closeWizard.bind(this)}
          wizardType={'add_namespace'}
        />
        <SetPreferenceModal
          isOpen={this.state.preferenceModal}
          toggleModal={this.togglePreferenceModal}
          onSuccess={this.onSystemPreferencesSaved}
          setAtSystemLevel={true}
        />
      </div>
    );
  }
}

export default Administration;
