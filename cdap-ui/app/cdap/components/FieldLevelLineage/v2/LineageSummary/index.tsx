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
import Table from '../Table';
import Header from '../Header';
import withStyles from '@material-ui/core/styles/withStyles';
import { Consumer } from '../Context/FllContext';

const styles = (theme) => {
  return {
    fllContainer: {
      paddingLeft: 100,
      paddingRight: 100,
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
          <div className={classes.fllContainer}>
            <div className="cause-col">
              <div className="cause-header" />
              <Header type="cause" first={firstCause} total={Object.keys(causeSets).length} />
              <div className="cause-tables col">
                {Object.keys(causeSets).map((key) => {
                  return <Table tableName={key} key={`cause ${key}`} nodes={causeSets[key]} />;
                })}
              </div>
            </div>
            <div className="target-col">
              <Header type="target" first={firstField} total={Object.keys(targetFields).length} />
              <div className="target-table col">
                <Table tableName={target} key="target" nodes={targetFields} />
              </div>
            </div>
            <div className="impact-col">
              <Header type="impact" first={firstImpact} total={Object.keys(impactSets).length} />
              <div className="impact-tables col">
                {Object.keys(impactSets).map((key) => {
                  return <Table tableName={key} key={`impact ${key}`} nodes={impactSets[key]} />;
                })}
              </div>
            </div>
          </div>
        );
      }}
    </Consumer>
  );
}

const StyledLineageSummary = withStyles(styles)(LineageSummary);

export default StyledLineageSummary;
