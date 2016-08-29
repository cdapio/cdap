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
require('./ServiceStatus.less');

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
    )
  };
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
    )
  };
}

export default ServiceStatusPanel;
