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
require('./Detail.less');

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
    )
  }
}

export default AdminDetailPanel;
