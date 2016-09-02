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
require('./InfoCard.less');
var classNames = require('classnames');

const propTypes = {
 version: PropTypes.string,
 uptime: PropTypes.object,
 isLoading: PropTypes.bool
};

function InfoCard({isLoading, uptime, version}) {

  var unitLoading = classNames("uptime-unit", {'hidden' : isLoading});
  var textLoading = classNames("info-card-text", {'hidden' : isLoading});
  var primaryText = '';
  var secondaryText = '';
  var unit;

  if(version){
    primaryText = version;
    secondaryText = 'Version';
  } else if(uptime){
    unit = <div className={unitLoading}> {uptime.unit} </div>;
    primaryText = uptime.duration;
    secondaryText = 'Uptime';
  }

  return (
  <div className="info-card">
    {unit}
    <i className={classNames("fa", "fa-spinner", "fa-spin", "fa-3x", {"hidden" : !isLoading})} />
    <div className={textLoading}>
      <div className="info-card-main-text">
        {primaryText}
      </div>
      <div className="info-card-secondary-text">
        {secondaryText}
      </div>
    </div>
  </div>
  );
}

InfoCard.propTypes = propTypes;

export default InfoCard;
