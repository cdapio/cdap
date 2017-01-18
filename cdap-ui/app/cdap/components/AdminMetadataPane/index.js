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
require('./AdminMetadataPane.scss');
import StatContainer from '../StatContainer/index.js';
import shortid from 'shortid';
import T from 'i18n-react';
import {humanReadableDate} from 'services/helpers';

function AdminMetadataPane({ statObject }) {

  if (!statObject || !statObject.stats) { return; }

  let containers = [];

  // Construct array of stats from those passed in as props - these stats are being passed in successfully
  let statsList = [];

  statObject.stats.forEach((stat) => {

    // Ignore Stats Associated with WritePointer, ReadPointer, and VisibilityUpperBound
    if (!(stat.statName === 'WritePointer' || stat.statName === 'ReadPointer' || stat.statName === 'VisibilityUpperBound')) {

      // Convert snapshot time to human readable number
      if (stat.statName === 'SnapshotTime') {
        stat.statNum = humanReadableDate(Math.floor(stat.statNum.split(',').join('')), true);

        statsList.push (
          <StatContainer
            label={T.translate(`features.Management.DetailPanel.labels.${stat.statName}`)}
            number={stat.statNum}
            date={true}
            key={shortid.generate()}
          />
        );
      } else {
        statsList.push (
          <StatContainer
            label={T.translate(`features.Management.DetailPanel.labels.${stat.statName}`)}
            number={stat.statNum}
            key={shortid.generate()}
          />
        );
      }
    }
  });

  // Construct Columns of Statistics
  for (let j = 0; statsList && j < statsList.length; j+=2) {
    let temp;

    if (j+1 < statsList.length) {
      temp = <div><span>{statsList[j]}</span><br/><span>{statsList[j+1]}</span></div>;
    } else {
      temp = statsList[j];
    }

    containers.push(
      <div className="stat-column" key={shortid.generate()}>
        {temp}
      </div>
    );
  }

  let headerText = statObject.statsHeader ? T.translate(`features.Management.DetailPanel.headers.${statObject.statsHeader}`) : <span className="fa fa-spinner" />;

  // Return the rendered content
  return (
    <div className="metadata-pane">
      <div className="pane-header">
        {headerText}
      </div>
      <div className="pane-body">
        {containers}
      </div>
    </div>
  );
}

AdminMetadataPane.propTypes = {
  statObject: PropTypes.shape({
    statsHeader : PropTypes.string,
    stats: PropTypes.arrayOf(
      PropTypes.shape({
        statName: PropTypes.string,
        statNum: PropTypes.oneOfType([
          PropTypes.string,
          PropTypes.number
        ])
      })
    )
  })
};

export default AdminMetadataPane;
