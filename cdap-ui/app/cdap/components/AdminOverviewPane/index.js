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
require('./AdminOverviewPane.scss');
import OverviewPaneCard from '../OverviewPaneCard/index.js';
var shortid = require('shortid');
import T from 'i18n-react';
import classnames from 'classnames';

const propTypes = {
  isLoading: PropTypes.bool,
  services: PropTypes.oneOfType([
    PropTypes.arrayOf(
      PropTypes.shape({
        name : PropTypes.string,
        version: PropTypes.string,
        url: PropTypes.string,
        logs: PropTypes.string
      })
    ),
    PropTypes.string
  ])
};

function AdminOverviewPane({services}) {
  let cards;
  let isEmpty = services.length === 1 && services[0].name === 'CDAP';
  cards = services
    .filter(service => service.name !== 'CDAP')
    .map((service) => {
      if (service.name !== 'CDAP') {
        return (
          <OverviewPaneCard
            key={shortid.generate()}
            name={service.name}
            version={service.version}
            url={service.url}
            logs={service.logs}
          />
        );
      }
    });

  return (
    <div className="overview-pane">
      <span>{T.translate('features.Administration.Component-Overview.label')}</span>
      <div className={classnames('overview-pane-container', {'empty-container': !cards.length})}>
      {
        isEmpty ?
          <h3>{T.translate('features.Administration.Component-Overview.emptyMessage')}</h3>
        :
          cards
      }
      </div>
    </div>
  );
}

AdminOverviewPane.propTypes = propTypes;

export default AdminOverviewPane;
