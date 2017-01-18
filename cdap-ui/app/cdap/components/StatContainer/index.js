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
require('./StatContainer.scss');
var classNames = require('classnames');

const propTypes = {
  number: PropTypes.oneOfType([
    PropTypes.string,
    PropTypes.number
  ]),
  date: PropTypes.bool,
  label: PropTypes.string,
  isLoading: PropTypes.bool
};

function StatContainer({number, label, date}) {
  let statClasses = date ? 'stat date-stat' : 'stat';
  return (
    <div className={classNames("stat-container")}>
      <div className={statClasses}>
        {number}
      </div>
      <div className="stat-label">
        {label}
      </div>
    </div>
  );
}

StatContainer.propTypes = propTypes;

export default StatContainer;
