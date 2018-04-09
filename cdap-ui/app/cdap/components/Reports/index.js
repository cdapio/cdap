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

import React from 'react';
import {Provider} from 'react-redux';
import {Route, Switch} from 'react-router-dom';
import ReportsStore from 'components/Reports/store/ReportsStore';
import ReportsList from 'components/Reports/ReportsList';
import ReportsDetail from 'components/Reports/ReportsDetail';

require('./Reports.scss');

export default function Reports() {
  return (
    <Provider store={ReportsStore}>
      <Switch>
        <Route exact path="/ns/:namespace/reports" component={ReportsList} />
        <Route exact path="/ns/:namespace/reports/:reportId" component={ReportsDetail} />
      </Switch>
    </Provider>
  );
}
