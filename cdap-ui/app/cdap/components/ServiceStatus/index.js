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

import React, {PropTypes} from 'react';
require('./ServiceStatus.less');
var classNames = require('classnames');

const propTypes = {
  name: PropTypes.string,
  status: PropTypes.string,
  isLoading: PropTypes.bool
};

function ServiceStatus({status, name, isLoading}) {
  var circle = '';

  if(!isLoading){
    switch(status){
        case 'Green' :
          circle = <div className={classNames({"status-circle-green" : !isLoading, "status-circle-grey" : isLoading})} />;
          break;
        case 'Yellow' :
          circle = <div className={classNames({"status-circle-yellow" : !isLoading, "status-circle-grey" : isLoading})} />;
          break;
        case 'Red' :
          circle = <div className={classNames({"status-circle-red" : !isLoading, "status-circle-grey" : isLoading})} />;
          break;
        default:
          circle = <div className="status-circle-grey" />;
    }
  } else {
    circle = <div className="status-circle-grey" />;
  }

  return (
    <div className="service-status">
      {circle}
      <div className="status-label">
        {name}
      </div>
    </div>
  );
}

ServiceStatus.propTypes = propTypes;

export default ServiceStatus;
