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

export default class Management extends Component {
  constructor(props) {
    super(props);
    this.props = props;
  }
  render() {
    return (
      <div className="Management">
        <ManagementView/>
      </div>
    );
  }
}

class ManagementView extends Component {
  render () {
    return (
      <div className="Management-View">
        <div className="admin-row">
          <InfoCard/>
          <InfoCard/>
          <ServiceStatusComponent/>
          <ServiceStatusPanel/>
        </div>
        <div className="admin-row">
          <AdminDetailPanel/>
        </div>
        <div className="navigation-container">
          <ul>
            <li className="one"><a>CDAP</a></li>
            <li className="two"><a>YARN</a></li>
            <li className="three"><a>HBASE</a></li>
            <hr/>
          </ul>
        </div>
        <ConfigureModule/>
        <OverviewModule/>
      </div>
    );
  }
}

class InfoCard extends Component {
  render () {
    return (
      <div className="Info-Card">
        <div className="Info-Card-Text">
          <div className="Info-Card-Main-Text">
            3.4
          </div>
          <div className="Info-Card-Secondary-Text">
            Version
          </div>
        </div>
      </div>
    );
  }
}
class ServiceStatusComponent extends Component {
  render() {
    return (
      <div className="Service-Status-Component">
        <div className="Service-Status-Panel-Label">
          <div className="Service-Status-Panel-Label-Text">
            Services
          </div>
        </div>
      </div>
    );
  }
}

class ServiceStatusPanel extends Component {
  render () {
    return (
      <div className="Service-Status-Panel">
        <ServiceStatus/>
        <ServiceStatus/>
        <ServiceStatus/>
        <ServiceStatus/>
        <ServiceStatus/>
        <ServiceStatus/>
        <ServiceStatus/>
        <ServiceStatus/>
        <ServiceStatus/>
      </div>
    );
  }
}

class ServiceStatus extends Component {
  render () {
    return (
      <div className="Service-Status">
        <div className="Status-Circle">
        </div>
        <div className="Status-Label">
          Metrics Processor
        </div>
      </div>
    );
  }
}

class AdminDetailPanel extends Component {
  render () {
    return (
      <div className="Admin-Detail-Panel">
        <div className="Admin-Detail-Panel-Button-Left">
          <i className="fa fa-chevron-left" aria-hidden="true"></i>
        </div>
        <div className="Admin-Detail-Panel-Button-Right">
          <i className="fa fa-chevron-right" aria-hidden="true"></i>
        </div>
        <div className="Admin-Detail-Panel-Header">
          <div className="Admin-Detail-Panel-Header-Name">
            YARN
          </div>
          <div className="Admin-Detail-Panel-Header-Status">
            Last updated 15 seconds ago
          </div>
        </div>
        <div className="Admin-Detail-Panel-Body">
          <DetailModule/>
          <DetailModule/>
          <DetailModule/>
        </div>
      </div>
    );
  }
}

class DetailModule extends Component {
  render () {
    return (
      <div className="Detail-Module">
        <div className="Detail-Module-Header">
          Nodes
        </div>
        <div className="Detail-Module-Body">
          <div className="row">
            <DetailModuleStatContainer/>
            <DetailModuleStatContainer/>
            <DetailModuleStatContainer/>
          </div>
          <div className="row">
            <DetailModuleStatContainer/>
            <DetailModuleStatContainer/>
            <DetailModuleStatContainer/>
          </div>
        </div>
      </div>
    );
  }
}

class DetailModuleStatContainer extends Component {
  render() {
    return (
      <div className="Detail-Module-Stat-Container">
        <div className="Detail-Module-Stat">
          25
        </div>
        <div className="Detail-Module-Stat-Label">
          Total
        </div>
      </div>
    );
  }
}

class ConfigureModule extends Component {
  render() {
    return (
      <div className="Configure-Module">
        <span>Configure</span>
        <div className="Configure-Module-Container">
          <ConfigureButton/>
          <ConfigureButton/>
          <ConfigureButton/>
          <ConfigureButton/>
          <ConfigureButton/>
          <ConfigureButton/>
          <ConfigureButton/>
          <ConfigureButton/>
          <ConfigureButton/>
          <ConfigureButton/>
          <ConfigureButton/>
          <ConfigureButton/>
          <ConfigureButton/>
          <ConfigureButton/>
          <ConfigureButton/>
          <ConfigureButton/>
        </div>
      </div>
    );
  }
}

class OverviewModule extends Component {
  render() {
    return (
      <div className="Overview-Module">
        <span>Component Overview</span>
        <div className="Overview-Module-Container">
          <div className="Overview-Card-Row">
            <OverviewModuleCard/>
            <OverviewModuleCard/>
            <OverviewModuleCard/>
          </div>
          <div className="Overview-Card-Row">
            <OverviewModuleCard/>
            <OverviewModuleCard/>
            <OverviewModuleCard/>
          </div>
          <div className="Overview-Card-Row">
            <OverviewModuleCard/>
            <OverviewModuleCard/>
            <OverviewModuleCard/>
          </div>
        </div>
      </div>
    );
  }
}

class OverviewModuleCard extends Component {
  render() {
    return (
      <div className="Overview-Module-Card">
        <div className="Overview-Module-Card-Header">
          <span className="Overview-Module-Card-Name">HBASE</span>
          <span className="Overview-Module-Card-Version">5.6</span>
        </div>
        <div className="Overview-Module-Card-Body">
          <div className="icon-container">
            <i className="fa fa-list-alt" aria-hidden="true">
            </i>
          </div>
          <div className="icon-container icon-container-right">
            <i className="fa fa-arrows-alt" aria-hidden="true"></i>
          </div>
        </div>
      </div>
    );
  }
}

class ConfigureButton extends Component {
  render() {
    return (
      <div className="Configure-Button">
        <i className="fa fa-arrows-alt"></i>
        <div className="Configure-Button-Text">
          View Configurations
        </div>
      </div>
    );
  }
}
