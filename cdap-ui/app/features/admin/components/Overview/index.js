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

import React, {Component, PropTypes} from 'react';
var classNames = require('classnames');
require('./Overview.less');

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

export default OverviewModule;
