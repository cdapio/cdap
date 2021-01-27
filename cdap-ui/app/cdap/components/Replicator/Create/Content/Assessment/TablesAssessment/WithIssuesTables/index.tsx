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
    noTablesText: {
      fontWeight: 600,
      padding: '5px 7px',
      borderBottom: `1px solid ${theme.palette.grey[500]}`,
    },
    errorBorder: {
      borderColor: theme.palette.red[100],
    },
  };
};

interface IIssuesTableProps extends WithStyles<typeof styles> {
  tables: ITable[];
  setOpenTable: (table: ITable) => void;
}

const WithIssuesTableView: React.FC<IIssuesTableProps> = ({ classes, tables, setOpenTable }) => {
  const borderClass =
    tables.length === 0
      ? null
      : {
          gridRow: classes.errorBorder,
        };
  return (
    <div className={classes.root}>
      <div className={classes.text}>
        {tables.length} tables have been assessed with schema issues
      </div>

      <Table columnTemplate="2fr 100px 2fr 100px 100px 100px 3fr">
        <TableHeader>
          <TableRow classes={borderClass}>
            <TableCell>Name</TableCell>
            <TableCell textAlign="right">Number of columns</TableCell>
            <TableCell />
            <TableCell textAlign="right">Data type issues</TableCell>
            <TableCell textAlign="right">Partially supported</TableCell>
            <TableCell textAlign="right">Not supported</TableCell>
            <TableCell />
          </TableRow>
        </TableHeader>

        <TableBody>
          {tables.map((row: ITable) => {
            return (
              <TableRow key={`${row.database}-${row.table}`} classes={borderClass}>
                <TableCell>{row.table}</TableCell>
                <TableCell textAlign="right">{row.numColumns}</TableCell>
                <TableCell />
                <TableCell textAlign="right">
                  {row.numColumnsPartiallySupported + row.numColumnsNotSupported}
                </TableCell>
                <TableCell textAlign="right">{row.numColumnsPartiallySupported}</TableCell>
                <TableCell textAlign="right">{row.numColumnsNotSupported}</TableCell>
                <TableCell textAlign="right">
                  <ViewMappingButton onClick={() => setOpenTable(row)} />
                </TableCell>
              </TableRow>
            );
          })}

          <If condition={tables.length === 0}>
            <div className={classes.noTablesText}>
              The system hasn't found any tables with schema issues
            </div>
          </If>
        </TableBody>
      </Table>
    </div>
  );
};

const WithIssuesTable = withStyles(styles)(WithIssuesTableView);
export default WithIssuesTable;
