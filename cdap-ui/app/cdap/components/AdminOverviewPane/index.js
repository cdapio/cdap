  /*
 * Copyright © 2016-2017 Cask Data, Inc.
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
import LoadingSVG from 'components/LoadingSVG';

const propTypes = {
  isLoading: PropTypes.bool,
  platforms: PropTypes.object
};

function AdminOverviewPane({platforms, isLoading}) {
  let cards = Object.keys(platforms)
    .filter(platform => platforms[platform].name !== 'CDAP')
    .map((platform) => {
      platform = platforms[platform];
      return (
        <OverviewPaneCard
          key={shortid.generate()}
          name={platform.name}
          version={platform.version}
          url={platform.url}
          logs={platform.logs}
        />
      );
    });

  const renderContents = () => {
    if (isLoading) {
      return (
        <LoadingSVG />
      );
    }

    if (!cards.length) {
      return (<h3>{T.translate('features.Administration.Component-Overview.emptyMessage')}</h3>);
    }

    return cards;
  };

  return (
    <div className="overview-pane">
      <span>{T.translate('features.Administration.Component-Overview.label')}</span>
      <div className={classnames('overview-pane-container', {'empty-container': !cards.length})}>
        { renderContents() }
      </div>
    </div>
  );
}

AdminOverviewPane.propTypes = propTypes;

export default AdminOverviewPane;
