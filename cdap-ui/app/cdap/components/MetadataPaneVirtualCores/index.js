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
require('./MetadataPaneVirtualCores.less');
import StatContainer from '../StatContainer/index.js';
var classNames = require('classnames');

const propTypes = {
  numStats: PropTypes.number,
  label: PropTypes.string,
  isLoading: PropTypes.bool
};

function MetadataPaneVirtualCores({numStats, label, isLoading}){

  var stats = [];

  for(var i = 0; i < numStats; i++){
    stats.push(<StatContainer isLoading={isLoading} number={25} label="Total" />);
  }

  var containers = [];

  for(var j = 0 ; j < stats.length; j+=2){
    var temp;

    if(j+1 < stats.length){
      temp = <div><span>{stats[j]}</span><br/><span>{stats[j+1]}</span></div>;
    }
    else {
      temp = stats[j];
    }

    containers.push(<div className="stat-container" key={j}>{temp}</div>);
  }

  return (
    <div className="metadata-pane-virtual-cores">
      <div className="pane-header">
        {label}
      </div>
      <div className={classNames("spinner-container", {"hidden" : !isLoading})}>
        <div className={classNames("fa", "fa-spinner", "fa-spin", "spinner", "fa-3x", {"hidden" : !isLoading})}></div>
      </div>
      <div className="pane-body">
        {containers}
      </div>
    </div>
  );
}

MetadataPaneVirtualCores.propTypes = propTypes;

export default MetadataPaneVirtualCores;
