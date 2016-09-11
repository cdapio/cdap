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

import AdminAppMetadataPane from '../AdminAppMetadataPane/index.js';
import AdminMemoryMetadataPane from '../AdminMemoryMetadataPane/index.js';
import AdminNodeMetadataPane from '../AdminNodeMetadataPane/index.js';
import AdminVirtualCoresMetadataPane from '../AdminVirtualCoresMetadataPane/index.js';

import React, {PropTypes} from 'react';
require('./AdminDetailPanel.less');

const propTypes = {
  applicationName: PropTypes.string,
  timeFromUpdate: PropTypes.number,
  isLoading: PropTypes.bool,
  clickLeftButton: PropTypes.func,
  clickRightButton: PropTypes.func
};

function AdminDetailPanel({applicationName, timeFromUpdate, clickLeftButton, clickRightButton}){
  return (
    <div className="admin-detail-panel">
      <div onClick={clickLeftButton} className="admin-detail-panel-button-left">
        <i className="fa fa-chevron-left" aria-hidden="true" />
      </div>
      <div onClick={clickRightButton} className="admin-detail-panel-button-right">
        <i className="fa fa-chevron-right" aria-hidden="true" />
      </div>
      <div className="admin-detail-panel-header">
        <div className="admin-detail-panel-header-name">
          {applicationName}
        </div>
        <div className="admin-detail-panel-header-status">
          Last updated {timeFromUpdate} seconds ago
        </div>
      </div>
      <div className="admin-detail-panel-body">
        <div className="end-line" />
        <AdminNodeMetadataPane />
        <div className="vertical-line" />
        <AdminVirtualCoresMetadataPane />
        <div className="vertical-line" />
        <AdminMemoryMetadataPane />
        <div className="vertical-line" />
        <AdminAppMetadataPane />
        <div className="end-line" />
      </div>
    </div>
  );
}

AdminDetailPanel.propTypes = propTypes;

export default AdminDetailPanel;
