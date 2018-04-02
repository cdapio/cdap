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
import ReportsStore from 'components/Reports/store/ReportsStore';
import ReportsList from 'components/Reports/ReportsList';

require('./Reports.scss');

export default function Reports() {
  return (
    <Provider store={ReportsStore}>
      <div className="reports-container">
        <div className="header">
          <div className="reports-view-options">
            <span className="active">Reports List</span>
            <span className="separator">|</span>
            <span>Reports View</span>
          </div>
        </div>

        <ReportsList />
      </div>
    </Provider>
  );
}
