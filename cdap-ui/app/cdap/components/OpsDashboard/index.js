/*
 * Copyright © 2018 Cask Data, Inc.
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

import React, { Component } from 'react';
import RunsGraph from 'components/OpsDashboard/RunsGraph';
import {getCurrentNamespace} from 'services/NamespaceStore';
import {Provider} from 'react-redux';
import DashboardStore from 'components/OpsDashboard/store/DashboardStore';
import RunsList from 'components/OpsDashboard/RunsList';

require('./OpsDashboard.scss');

export default class OpsDashboard extends Component {
  render() {
    return (
      <Provider store={DashboardStore}>
        <div className="ops-dashboard">
          <div className="header clearfix">
            <div className="links float-xs-left">
              <span className="active">Run Monitor</span>
              <span className="separator">|</span>
              <span>Reports</span>
            </div>

            <div className="namespace-picker float-xs-right">
              <div className="namespace-list">
                Monitor Namespace <strong>{getCurrentNamespace()}</strong>
              </div>
              <div className="monitor-more">
                Monitor More
              </div>
            </div>
          </div>

          <RunsGraph />
          <RunsList />
        </div>
      </Provider>
    );
  }
}
