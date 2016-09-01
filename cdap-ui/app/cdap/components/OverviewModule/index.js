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
require('./OverviewModule.less');
import OverviewModuleCard from '../OverviewModuleCard/index.js';
var classNames = require('classnames');

export default function OverviewModule({isLoading}) {

  var cards = [];
  for(var i = 0; i < 8; i++){
    cards.push(<OverviewModuleCard isLoading={isLoading} name="HBASE" version={5.6}/>);
  }

  return (
    <div className="overview-module">
      <span>Component Overview</span>
      <div className="overview-module-container">
        <div className={classNames("spinner-container", { "hidden" : !isLoading})}>
          <div className={classNames("fa", "fa-spinner", "fa-spin", "spinner", "fa-3x", { "hidden" : !isLoading})}></div>
        </div>
       {cards}
      </div>
    </div>
  );
}

OverviewModule.propTypes = {
  isLoading: PropTypes.bool
};
