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
import Table from 'components/Table';
import TableHeader from 'components/Table/TableHeader';
import TableRow from 'components/Table/TableRow';
import TableCell from 'components/Table/TableCell';
import TableBody from 'components/Table/TableBody';
import ViewMappingButton from 'components/Replicator/Create/Content/Assessment/TablesAssessment/ViewMappingButton';

const styles = (theme): StyleRules => {
  return {
    text: {
      marginBottom: '10px',
      color: theme.palette.grey[100],
    },
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

      <Table columnTemplate="250px 100px 1fr">
        <TableHeader>
          <TableRow>
            <TableCell>Name</TableCell>
            <TableCell textAlign="right">Number of columns</TableCell>
            <TableCell />
          </TableRow>
        </TableHeader>

        <TableBody>
          {tables.map((row: ITable) => {
            return (
              <TableRow key={`${row.database}-${row.table}`}>
                <TableCell>{row.table}</TableCell>
                <TableCell textAlign="right">{row.numColumns}</TableCell>
                <TableCell textAlign="right">
                  <ViewMappingButton onClick={() => setOpenTable(row)} />
                </TableCell>
              </TableRow>
            );
          })}
        </TableBody>
      </Table>
    </div>
  );
};

const NoIssuesTables = withStyles(styles)(NoIssuesTablesView);
export default NoIssuesTables;
