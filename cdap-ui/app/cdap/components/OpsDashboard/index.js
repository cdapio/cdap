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
import {Provider} from 'react-redux';
import DashboardStore, {DashboardActions} from 'components/OpsDashboard/store/DashboardStore';
import RunsList from 'components/OpsDashboard/RunsList';
import {getData} from 'components/OpsDashboard/store/ActionCreator';
import NamespacesPicker from 'components/NamespacesPicker';
import {setNamespacesPick} from 'components/OpsDashboard/store/ActionCreator';

require('./OpsDashboard.scss');

export default class OpsDashboard extends Component {
  componentWillMount() {
    getData();
  }

  componentWillUnmount() {
    DashboardStore.dispatch({
      type: DashboardActions.reset
    });
  }

  render() {
    return (
      <Provider store={DashboardStore}>
        <div className="ops-dashboard">
          <div className="header clearfix">
            <div className="links float-xs-left">
              <span>Dashboard</span>
            </div>

            <NamespacesPicker setNamespacesPick={setNamespacesPick} />
          </div>

          <RunsGraph />
          <RunsList />
        </div>
      </Provider>
    );
  }
}
