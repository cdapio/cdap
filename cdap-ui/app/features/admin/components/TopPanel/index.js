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
require('./TopPanel.less');

class TopPanel extends Component {
  render () {
    return (
      <div className="Top-Panel">
        <div className="admin-row">
          <InfoCard/>
          <InfoCard/>
          <div className="Service-Status-Component">
            <div className="Service-Status-Panel-Label">
              <div className="Service-Status-Panel-Label-Text">
                Services
              </div>
            </div>
          </div>
          <ServiceStatusPanel/>
        </div>
        <div className="admin-row">
          <AdminDetailPanel/>
        </div>
        <div className="navigation-container">
          <ul>
            <li className="one active"><a href="#">CDAP</a></li>
            <li className="two"><a href="#">YARN</a></li>
            <li className="three"><a href="#">HBASE</a></li>
            <hr/>
          </ul>
        </div>
        <ConfigureModule/>
        <OverviewModule/>
      </div>
    )
  };
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
    )
  };
}

export default TopPanel;