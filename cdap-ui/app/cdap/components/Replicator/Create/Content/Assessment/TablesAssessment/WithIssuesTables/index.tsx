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
import If from 'components/If';
import { getSchemaTableStyles } from 'components/Replicator/Create/Content/Assessment/tableStyles';

const styles = (theme): StyleRules => {
  return {
    noTablesText: {
      fontWeight: 600,
      padding: '5px 7px',
      borderBottom: `1px solid ${theme.palette.grey[500]}`,
    },
    ...getSchemaTableStyles(theme, '2fr 100px 2fr 100px 100px 100px 3fr'),
  };
};

interface IIssuesTableProps extends WithStyles<typeof styles> {
  tables: ITable[];
  setOpenTable: (table: ITable) => void;
}

const WithIssuesTableView: React.FC<IIssuesTableProps> = ({ classes, tables, setOpenTable }) => {
  return (
    <div className={classes.root}>
      <div className={classes.text}>
        {tables.length} tables have been assessed with schema issues
      </div>

      <div className={`grid-wrapper ${classes.gridWrapper}`}>
        <div className="grid grid-container grid-compact">
          <div className="grid-header">
            <div className="grid-row">
              <div>Name</div>
              <div>Number of columns</div>
              <div />
              <div>Data type issues</div>
              <div>Partially supported</div>
              <div>Not supported</div>
              <div />
            </div>
          </div>

          <div className="grid-body">
            {tables.map((row: ITable) => {
              return (
                <div key={`${row.database}-${row.table}`} className="grid-row">
                  <div>{row.table}</div>
                  <div>{row.numColumns}</div>
                  <div />
                  <div>{row.numColumnsPartiallySupported + row.numColumnsNotSupported}</div>
                  <div>{row.numColumnsPartiallySupported}</div>
                  <div>{row.numColumnsNotSupported}</div>
                  <div>
                    <span className={classes.mappingButton} onClick={() => setOpenTable(row)}>
                      View mappings
                    </span>
                  </div>
                </div>
              );
            })}
          </div>

          <If condition={tables.length === 0}>
            <div className={classes.noTablesText}>
              The system hasn't found any tables with schema issues
            </div>
          </If>
        </div>
      </div>
    </div>
  );
};

const WithIssuesTable = withStyles(styles)(WithIssuesTableView);
export default WithIssuesTable;
