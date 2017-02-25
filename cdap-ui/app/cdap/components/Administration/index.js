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
require('./Administration.scss');
import InfoCard from '../InfoCard';
import ServiceLabel from '../ServiceLabel';
import ServiceStatusPanel from '../ServiceStatusPanel';
import AdminDetailPanel from '../AdminDetailPanel';
import AdminConfigurePane from '../AdminConfigurePane';
import AdminOverviewPane from '../AdminOverviewPane';
import AbstractWizard from '../AbstractWizard';
import SetPreferenceModal from '../FastAction/SetPreferenceAction/SetPreferenceModal';
import Helmet from 'react-helmet';
import NamespaceStore from 'services/NamespaceStore';
import VersionStore from 'services/VersionStore';
import VersionActions from 'services/VersionStore/VersionActions';
import MyCDAPVersionApi from 'api/version';
import {MyServiceProviderApi} from 'api/serviceproviders';
import Mousetrap from 'mousetrap';
import moment from 'moment';

import T from 'i18n-react';
var shortid = require('shortid');
var classNames = require('classnames');

class Administration extends Component {

  constructor(props) {
    super(props);
    this.state = {
      application: '',
      applications: [],
      services: [],
      version: '',
      loading: false,
      lastUpdated : 0,
      uptime: 0,
      wizard : {
        actionIndex : null,
        actionType : null
      },
      preferenceModal: false,
      serviceProviders: [],
      serviceData: {}
    };

    MyServiceProviderApi.pollList()
      .subscribe(
        (res) => {
          let apps = [];
          let services = [];
          for (let key in res) {
            if (res.hasOwnProperty(key)) {
              apps.push(key);
              if (key !== 'cdap') {
                services.push({
                  name: T.translate(`features.Administration.Component-Overview.headers.${key}`),
                  version: res[key].Version,
                  url: res[key].WebURL,
                  logs: res[key].LogsURL
                });
              } else {
                // Uptime is attached to cdap response object
                let uptime = res[key].Uptime;
                let tempTime = moment.duration(uptime);
                let days = tempTime.days() < 10 ? '0' + tempTime.days() : tempTime.days();
                let hours = tempTime.hours() < 10 ? '0' + tempTime.hours() : tempTime.hours();
                let minutes = tempTime.minutes() < 10 ? '0' + tempTime.minutes() : tempTime.minutes();
                let time = `${days}:${hours}:${minutes}`;
                this.setState({uptime: time});
                services.push({
                  name: T.translate(`features.Administration.Component-Overview.headers.${key}`)
                });
              }
            }
          }
          this.getServices(apps);
          if (!this.state.application) {
            this.setState({
              application : apps[0],
              applications : apps,
              services : !services.length ? 'empty' : services
            });
          } else {
            this.setState({
              applications : apps,
              services : !services.length ? 'empty' : services
            });
          }
        }
      );

    this.updatingInterval;
    this.lastAccessedNamespace;
    this.interval = undefined;
    this.getServices = this.getServices.bind(this);
    this.clickLeft = this.clickLeft.bind(this);
    this.clickRight = this.clickRight.bind(this);
    this.setToContext = this.setToContext.bind(this);
    this.openNamespaceWizard = this.openNamespaceWizard.bind(this, 0, 'add_namespace');
    this.togglePreferenceModal = this.togglePreferenceModal.bind(this);
    this.onSystemPreferencesSaved = this.onSystemPreferencesSaved.bind(this);
    this.closePreferencesSavedMessage = this.closePreferencesSavedMessage.bind(this);
  }

  componentDidMount() {
    if (!VersionStore.getState().version) {
      MyCDAPVersionApi.get().subscribe((res) => {
        this.setState({ version : res.version });
        VersionStore.dispatch({
          type: VersionActions.updateVersion,
          payload: {
            version: res.version
          }
        });
      });
    } else {
      this.setState({ version : VersionStore.getState().version });
    }

    document.querySelector('#header-namespace-dropdown').style.display = 'none';
    this.lastAccessedNamespace = NamespaceStore.getState().selectedNamespace;
    Mousetrap.bind('left', this.clickLeft);
    Mousetrap.bind('right', this.clickRight);
  }

  componentWillUnmount() {
    document.querySelector('#header-namespace-dropdown').style.display = 'inline-block';
    clearInterval(this.updatingInterval);
  }

  onSystemPreferencesSaved() {
    this.setState({
      preferencesSaved: true
    });
    setTimeout(() => {
      this.setState({
        preferencesSaved: false
      });
    }, 3000);
  }

  closePreferencesSavedMessage() {
    this.setState({
      preferencesSaved: false
    });
  }

  // Retrieve the data for each service
  getServices(names) {
    let serviceData = {};
    names.forEach((name) => {
      MyServiceProviderApi.get({
        serviceprovider : name
      })
      .subscribe( (res) => {
        serviceData[name] = res;
        this.setState({
          serviceData : serviceData
        });

        clearInterval(this.updatingInterval);

        this.setState({
          lastUpdated : 0
        });

        this.updatingInterval= setInterval(()=> {
          this.setState({
            lastUpdated : this.state.lastUpdated + 30
          });
        }, 30000);
      });
    });
  }


  clickLeft() {
    var index = this.state.applications.indexOf(this.state.application);
    if (index === -1 || index === 0) {
      return;
    }
    this.setToContext(this.state.applications[index-1]);
  }

  clickRight() {
    var index = this.state.applications.indexOf(this.state.application);
    if (index === -1 || index === this.state.applications.length-1) {
      return;
    }
    this.setToContext(this.state.applications[index+1]);
  }

  setToContext(contextName) {
    if (this.state.application !== contextName) {

      this.setState({
        application: contextName
      });

    }
  }

  closeWizard() {
      this.setState({
        wizard: {
          actionIndex: null,
          actionType: null
        }
      });
  }

  openNamespaceWizard(index, type) {
    this.setState({
      wizard: {
        actionIndex: index,
        actionType: type
      }
    });
  }

  togglePreferenceModal() {
    this.setState({
      preferenceModal: !this.state.preferenceModal
    });
  }

  render () {
    // Constructs the Services Navigation
    var navItems = this.state.applications.map( (item) => {
      return (
        <li
          className={classNames({'active' : this.state.application === item})}
          key={shortid.generate()}
          onClick={this.setToContext.bind(this, item)}
        >
          {T.translate(`features.Administration.Component-Overview.headers.${item}`)}
        </li>
      );
    });

    return (
       <div className="administration">
        <Helmet
          title={T.translate('features.Administration.Title')}
        />
        <div className="top-panel">
          <div className="page-title-and-version">
            <span className="page-title">
              <h3>{T.translate('features.Administration.Title')}</h3>
            </span>
            <span className="cdap-version-label">
              {T.translate('features.Administration.Top.version-label')} - {this.state.version}
            </span>
          </div>
          <div className="admin-row top-row">
            <InfoCard
              isLoading={this.state.loading}
              primaryText={this.state.uptime}
              primaryLabelOne={T.translate('features.Administration.Top.primaryLabelOne')}
              primaryLabelTwo={T.translate('features.Administration.Top.primaryLabelTwo')}
              primaryLabelThree={T.translate('features.Administration.Top.primaryLabelThree')}
              secondaryText={T.translate('features.Administration.Top.time-label')}
            />
            <ServiceLabel/>
            <ServiceStatusPanel
              isLoading={this.state.loading}
            />
          </div>
          <div className="container">
            <ul className="nav nav-pills nav-justified centering-container">
                {navItems}
            </ul>
          </div>
          <hr className="admin-horizontal-line" />
          <div className="admin-row">
            <AdminDetailPanel
              isLoading={this.state.loading}
              applicationName={this.state.application}
              timeFromUpdate={this.state.lastUpdated}
              clickLeftButton={this.clickLeft}
              clickRightButton={this.clickRight}
              serviceData={this.state.serviceData[this.state.application]}
            />
          </div>
        </div>
        <hr className="admin-horizontal-line" />
        <div className="admin-bottom-panel">
          <AdminConfigurePane
            openNamespaceWizard={this.openNamespaceWizard}
            openPreferenceModal={this.togglePreferenceModal}
            preferencesSavedState={this.state.preferencesSaved}
            closePreferencesSavedMessage={this.closePreferencesSavedMessage}
          />
          <AdminOverviewPane
            isLoading={this.state.loading}
            services={this.state.services}
          />
        </div>
        <AbstractWizard
          isOpen={this.state.wizard.actionIndex !== null && this.state.wizard.actionType !== null}
          onClose={this.closeWizard.bind(this)}
          wizardType={this.state.wizard.actionType}
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
