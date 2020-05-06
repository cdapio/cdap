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
import { ITable } from 'components/Replicator/Create/Content/Assessment/TablesAssessment';
import { getSchemaTableStyles } from 'components/Replicator/Create/Content/Assessment/tableStyles';

const styles = (theme): StyleRules => {
  return {
    ...getSchemaTableStyles(theme, '250px 100px 1fr'),
  };
};

interface INoIssuesTablesProps extends WithStyles<typeof styles> {
  tables: ITable[];
  setOpenTable: (table: ITable) => void;
}

const NoIssuesTablesView: React.FC<INoIssuesTablesProps> = ({ classes, tables, setOpenTable }) => {
  if (tables.length === 0) {
    return null;
  }

  return (
    <div className={classes.root}>
      <div className={classes.text}>
        {tables.length} tables have been assessed with no schema issues
      </div>

      <div className={`grid-wrapper ${classes.gridWrapper}`}>
        <div className="grid grid-container grid-compact">
          <div className="grid-header">
            <div className="grid-row">
              <div>Name</div>
              <div>Number of columns</div>
              <div />
            </div>
          </div>

          <div className="grid-body">
            {tables.map((row: ITable) => {
              return (
                <div key={`${row.database}-${row.table}`} className="grid-row">
                  <div>{row.table}</div>
                  <div>{row.numColumns}</div>
                  <div>
                    <span className={classes.mappingButton} onClick={() => setOpenTable(row)}>
                      View mappings
                    </span>
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      </div>
    </div>
  );
};

const NoIssuesTables = withStyles(styles)(NoIssuesTablesView);
export default NoIssuesTables;
