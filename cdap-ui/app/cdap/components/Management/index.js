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
require('./Management.less');
import InfoCard from '../InfoCard';
import ServiceLabel from '../ServiceLabel';
import ServiceStatusPanel from '../ServiceStatusPanel';
import AdminDetailPanel from '../AdminDetailPanel';
import AdminConfigurePane from '../AdminConfigurePane';
import AdminOverviewPane from '../AdminOverviewPane';
import AbstractWizard from 'components/AbstractWizard';
import Helmet from 'react-helmet';
import NamespaceStore from 'services/NamespaceStore';
import {MyServiceProviderApi} from 'api/serviceproviders';
import Mousetrap from 'mousetrap';
import MyCDAPVersionApi from 'api/version.js';

import T from 'i18n-react';
var shortid = require('shortid');
var classNames = require('classnames');

var dummyData = {
  uptime: {
    duration: '--',
    unit: '-'
  }
};

class Management extends Component {

  constructor(props) {
    super(props);
    this.state = {
      application: '',
      applications: [],
      services: [],
      version: '',
      loading: false,
      lastUpdated : 0,
      wizard : {
        actionIndex : null,
        actionType : null
      },
      serviceProviders: [],
      serviceData: {}
    };

    MyServiceProviderApi.pollList()
      .subscribe(
        (res) => {
          let apps = [];
          let services = [];
          for(let key in res){
            if(res.hasOwnProperty(key)){
              apps.push(key);
              services.push({
                name: T.translate(`features.Management.Component-Overview.headers.${key}`),
                version: res[key].Version,
                url: res[key].WebURL,
                logs: res[key].LogsURL
              });
            }
          }
          this.getServices(apps);
          let current = apps[0];
          this.setState({
            application : current,
            applications : apps,
            services : services
          });
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
  }

  //Retrieve the data for each service
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
  componentWillMount(){
    MyCDAPVersionApi.get().subscribe((res) => this.setState({ version : res.version }));
  }
  componentDidMount(){
    document.querySelector('#header-namespace-dropdown').style.display = 'none';
    this.lastAccessedNamespace = NamespaceStore.getState().selectedNamespace;
    Mousetrap.bind('left', this.clickLeft);
    Mousetrap.bind('right', this.clickRight);
  }
  componentWillUnmount(){
    document.querySelector('#header-namespace-dropdown').style.display = 'inline-block';
  }
  clickLeft() {
    var index = this.state.applications.indexOf(this.state.application);
    if(index === -1 || index === 0){
      return;
    }
    this.setToContext(this.state.applications[index-1]);
  }

  clickRight() {
    var index = this.state.applications.indexOf(this.state.application);
    if(index === -1 || index === this.state.applications.length-1){
      return;
    }
    this.setToContext(this.state.applications[index+1]);
  }

  setToContext(contextName) {

    if(this.state.application !== contextName){

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

  render () {
    //Constructs the Services Navigation
    var navItems = this.state.applications.map( (item) => {
      return (
        <li
          className={classNames({'active' : this.state.application === item})}
          key={shortid.generate()}
          onClick={this.setToContext.bind(this, item)}
        >
          {T.translate(`features.Management.Component-Overview.headers.${item}`)}
        </li>
      );
    });

    return (
       <div className="management">
        <Helmet
          title={T.translate('features.Management.Title')}
        />
        <div className="top-panel">
          <div className="admin-row top-row">
            <InfoCard
              isLoading={this.state.loading}
              primaryText={this.state.version}
              secondaryText={T.translate('features.Management.Top.version-label')}
            />
            <InfoCard
              isLoading={this.state.loading}
              primaryText={dummyData.uptime.duration}
              secondaryText={T.translate('features.Management.Top.time-label')}
              superscriptText={dummyData.uptime.unit}
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
          <AdminConfigurePane openNamespaceWizard={this.openNamespaceWizard}/>
          <AdminOverviewPane
            isLoading={this.state.loading}
            services={this.state.services}
          />
        </div>
        <AbstractWizard
          isOpen={this.state.wizard.actionIndex !== null && this.state.wizard.actionType !== null}
          onClose={this.closeWizard.bind(this)}
          wizardType={this.state.wizard.actionType}
          backdrop={true}
        />
      </div>
    );
  }
}

export default Management;
