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
require('./InfoCard.less');
var classNames = require('classnames');

class InfoCard extends Component {

  render () {

    var unitLoading = classNames("uptime-unit", {'hidden' : this.props.isLoading});
    var textLoading = classNames("info-card-text", {'hidden' : this.props.isLoading});
    var primaryText = '';
    var secondaryText = '';

    if(this.props.version){
      primaryText = this.props.version;
      secondaryText = 'Version';
    } else if(this.props.uptime){
      this.unit = <div className={unitLoading}> {this.props.uptime.unit} </div>;
      primaryText = this.props.uptime.duration;
      secondaryText = 'Uptime';
    }

    return (
      <div className="info-card">
        {this.unit}
        <i className={classNames("fa", "fa-spinner", "fa-spin", "fa-3x", {"hidden" : !this.props.isLoading})}></i>
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
}

 InfoCard.propTypes = {
  version: PropTypes.string,
  uptime: {
    duration: PropTypes.number,
    unit: PropTypes.string
  },
  isLoading: PropTypes.bool
};

export default InfoCard;
