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
import PropTypes from 'prop-types';
import AdminOverviewPane from 'components/AdminOverviewPane';
import T from 'i18n-react';
import PlatformsDetails from 'components/Administration/AdminManagementTabContent/PlatformsDetails';
import ServicesTable from 'components/Administration/AdminManagementTabContent/ServicesTable';

const PREFIX = 'features.Administration';
require('./AdminManagementTabContent.scss');

export default function AdminManagementTabContent(props) {
  return (
    <div className="admin-management-tab-content">
      <div className="services-details">
        <div className="services-table-section">
          <strong> {T.translate(`${PREFIX}.Services.title`)} </strong>
          <ServicesTable />
        </div>
        <div className="platform-section">
          <PlatformsDetails platforms={props.platformsDetails} />
        </div>
      </div>
      <div className="admin-bottom-panel">
        <AdminOverviewPane
          isLoading={props.loading}
          platforms={props.platformsDetails}
        />
      </div>
    </div>
  );
}

AdminManagementTabContent.propTypes = {
  platformsDetails: PropTypes.object,
  loading: PropTypes.bool
};
