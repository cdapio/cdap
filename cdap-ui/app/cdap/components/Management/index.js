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
import Redirect from 'react-router/Redirect';
import Helmet from 'react-helmet';
import NamespaceStore from 'services/NamespaceStore';

import T from 'i18n-react';
var shortid = require('shortid');
var classNames = require('classnames');

var dummyData = {
  version: '4.0-SNAPSHOT',
  uptime: {
    duration: '0.2',
    unit: 'hr'
  },
  services: [
    {
      name: 'App Fabric',
      status: 'Green'
    },
    {
      name: 'Explore',
      status: 'Green'
    },
    {
      name: 'Metadata',
      status: 'Green'
    },
    {
      name: 'Metrics Processor',
      status: 'Green'
    },
    {
      name: 'Tephra Transaction',
      status: 'Green'
    },
    {
      name: 'Dataset Executor',
      status: 'Green'
    },
    {
      name: 'Log Saver',
      status: 'Green'
    },
    {
      name: 'Streams',
      status: 'Green'
    }
  ]
};

class Management extends Component {

  constructor(props) {
    super(props);
    this.state = {
      application: 'CDAP',
      lastUpdated: 15,
      loading: false,
      redirectTo: false,
      wizard : {
        actionIndex : null,
        actionType : null
      }
    };

    this.unsub;
    this.lastAccessedNamespace;
    this.interval = undefined;
    this.clickLeft = this.clickLeft.bind(this);
    this.clickRight = this.clickRight.bind(this);
    this.setToContext = this.setToContext.bind(this);
    this.openNamespaceWizard = this.openNamespaceWizard.bind(this, 0, 'add_namespace');
    this.applications = ['CDAP', 'YARN', 'HBASE'];
  }

  componentDidMount(){
    this.openNamespaceWizard();
    this.lastAccessedNamespace = NamespaceStore.getState().selectedNamespace;
  }
  clickLeft() {
    var index = this.applications.indexOf(this.state.application);
    if(index === -1 || index === 0){
      return;
    }
    this.setToContext(this.applications[index-1]);
  }

  clickRight() {
    var index = this.applications.indexOf(this.state.application);
    if(index === -1 || index === this.applications.length-1){
      return;
    }
    this.setToContext(this.applications[index+1]);
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
        },
        redirectTo: true
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

    var navItems = this.applications.map( (item) => {
      return (
        <li
          className={classNames({'active' : this.state.application === item})}
          key={shortid.generate()}
          onClick={this.setToContext.bind(this, item)}
        >
          {item}
        </li>
      );
    });
    let redirectUrl = this.lastAccessedNamespace ? `/ns/${this.lastAccessedNamespace}` : '/';
    return (
       <div className="management">
        {
          this.state.redirectTo && <Redirect to={redirectUrl} />
        }
        <Helmet
          title={T.translate('features.Management.Title')}
        />
        <div className="top-panel">
          <div className="admin-row top-row">
            <InfoCard
              isLoading={this.state.loading}
              primaryText={dummyData.version}
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
              services={dummyData.services}
            />
          </div>
          <div className="admin-row">
            <AdminDetailPanel
              isLoading={this.state.loading}
              applicationName={this.state.application}
              timeFromUpdate={this.state.lastUpdated}
              clickLeftButton={this.clickLeft}
              clickRightButton={this.clickRight}
            />
          </div>
          <div className="container">
            <ul className="nav nav-pills nav-justified centering-container">
                {navItems}
            </ul>
          </div>
        </div>
        <div className="admin-bottom-panel">
          <AdminConfigurePane openNamespaceWizard={this.openNamespaceWizard}/>
          <AdminOverviewPane isLoading={this.state.loading} />
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
