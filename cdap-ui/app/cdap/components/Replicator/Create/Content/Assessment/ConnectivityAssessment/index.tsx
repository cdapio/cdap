/*
 * Copyright © 2020 Cask Data, Inc.
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

import * as React from 'react';
import withStyles, { WithStyles, StyleRules } from '@material-ui/core/styles/withStyles';
import { getGenericIssuesTableStyles } from 'components/Replicator/Create/Content/Assessment/tableStyles';

const styles = (theme): StyleRules => {
  return getGenericIssuesTableStyles(theme);
};

interface IConnectivityAssessmentProps extends WithStyles<typeof styles> {
  connectivity: any;
}

const ConnectivityAssessmentView: React.FC<IConnectivityAssessmentProps> = ({
  classes,
  connectivity,
}) => {
  if (connectivity.length === 0) {
    return <div>No connectivity issue</div>;
  }

  return (
    <div className={classes.root}>
      <div className={classes.text}>Address the following connectivity issues</div>
      <div className={`grid-wrapper ${classes.gridWrapper}`}>
        <div className="grid grid-container grid-compact">
          <div className="grid-header">
            <div className="grid-row">
              <div>Issue name</div>
              <div>Description</div>
              <div>Suggestion</div>
              <div>Impact</div>
            </div>
          </div>

          <div className="grid-body">
            {connectivity.map((conn, i) => {
              return (
                <div key={`${conn.name}${i}`} className="grid-row">
                  <div>{conn.name}</div>
                  <div>{conn.description || '--'}</div>
                  <div>{conn.suggestion || '--'}</div>
                  <div>{conn.impact || '--'}</div>
                </div>
              );
            })}
          </div>
        </div>
      </div>
    </div>
  );
};

const ConnectivityAssessment = withStyles(styles)(ConnectivityAssessmentView);
export default ConnectivityAssessment;
