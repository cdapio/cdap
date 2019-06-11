/*
 * Copyright © 2019 Cask Data, Inc.
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
import FllHeader from 'components/FieldLevelLineage/v2/FllHeader';
import FllTable from 'components/FieldLevelLineage/v2/FllTable';
import withStyles from '@material-ui/core/styles/withStyles';
import { Consumer } from 'components/FieldLevelLineage/v2/Context/FllContext';

const styles = (theme) => {
  return {
    root: {
      paddingLeft: '100px',
      paddingRight: '100px',
      display: 'flex',
      justifyContent: 'space-between',
    },
  };
};

function LineageSummary({ classes }) {
  return (
    <Consumer>
      {({ causeSets, target, targetFields, impactSets, firstCause, firstImpact, firstField }) => {
        return (
          <div className={classes.root}>
            <div>
              <FllHeader type="cause" first={firstCause} total={Object.keys(causeSets).length} />
              {Object.keys(causeSets).map((key) => {
                return <FllTable key={key} tableName={key} fields={causeSets[key]} />;
              })}
            </div>
            <div>
              <FllHeader
                type="target"
                first={firstField}
                total={Object.keys(targetFields).length}
              />
              <FllTable key={target} tableName={target} fields={targetFields} />
            </div>
            <div>
              <FllHeader type="impact" first={firstImpact} total={Object.keys(impactSets).length} />
              {Object.keys(impactSets).map((key) => {
                return <FllTable key={key} tableName={key} fields={impactSets[key]} />;
              })}
            </div>
          </div>
        );
      }}
    </Consumer>
  );
}

const StyledLineageSummary = withStyles(styles)(LineageSummary);

export default StyledLineageSummary;
