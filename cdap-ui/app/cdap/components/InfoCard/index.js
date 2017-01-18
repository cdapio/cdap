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
require('./InfoCard.scss');
var classNames = require('classnames');

const propTypes = {
  primaryText: PropTypes.oneOfType([
    PropTypes.string,
    PropTypes.number
  ]),
  primaryLabel: PropTypes.string,
  secondaryText: PropTypes.string,
  superscriptText: PropTypes.string,
  isLoading: PropTypes.bool,
  primaryLabelOne: PropTypes.string,
  primaryLabelTwo: PropTypes.string,
  primaryLabelThree: PropTypes.string
};

function InfoCard({isLoading, primaryText, primaryLabel, primaryLabelOne, primaryLabelTwo, primaryLabelThree, secondaryText}) {
  return (
    <div className="info-card">
      <i className={classNames("fa", "fa-spinner", "fa-spin", "fa-2x", {"hidden" : !isLoading})} />
      <div className={classNames("info-card-text", {'hidden' : isLoading})}>
        <div className="info-card-main-text">
          {primaryText}
        </div>
        <div className="primary-label">
            {primaryLabel}
          <span>
            {primaryLabelOne}
          </span>
          <span>
            {primaryLabelTwo}
          </span>
          <span>
            {primaryLabelThree}
          </span>
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
