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
require('./OverviewPaneCard.less');

const propTypes = {
  name: PropTypes.string,
  version: PropTypes.number
};

function OverviewPaneCard({name, version}) {
  return (
    <div className="overview-pane-card">
      <div className="overview-pane-card-header">
        <span className="overview-pane-card-name">
          {name}
        </span>
        <span className="overview-pane-card-version">
          {version}
        </span>
      </div>
      <div className="overview-pane-card-body">
        <div className="icon-container">
          <i className="fa fa-list-alt" aria-hidden="true">
          </i>
        </div>
        <div className="icon-container icon-container-right">
          <i className="fa fa-arrows-alt" aria-hidden="true" />
        </div>
      </div>
    </div>
  );
}

OverviewPaneCard.propTypes = propTypes;

export default OverviewPaneCard;
