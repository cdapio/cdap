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

interface IFeaturesAssessmentProps extends WithStyles<typeof styles> {
  features: any;
}

const FeaturesAssessmentView: React.FC<IFeaturesAssessmentProps> = ({ classes, features }) => {
  if (features.length === 0) {
    return <div>No missing features</div>;
  }

  return (
    <div className={classes.root}>
      <div className={classes.text}>Address the following missing features</div>
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
            {features.map((feature, i) => {
              return (
                <div key={`${feature.name}${i}`} className="grid-row">
                  <div>{feature.name}</div>
                  <div>{feature.description || '--'}</div>
                  <div>{feature.suggestion || '--'}</div>
                  <div>{feature.impact || '--'}</div>
                </div>
              );
            })}
          </div>
        </div>
      </div>
    </div>
  );
};

const FeaturesAssessment = withStyles(styles)(FeaturesAssessmentView);
export default FeaturesAssessment;
