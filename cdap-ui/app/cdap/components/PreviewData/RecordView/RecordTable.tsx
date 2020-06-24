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

import React from 'react';
import Table from '@material-ui/core/Table';
import TableBody from '@material-ui/core/TableBody';
import TableRow from '@material-ui/core/TableRow';
import Paper from '@material-ui/core/Paper';
import TableHead from '@material-ui/core/TableHead';
import withStyles, { WithStyles } from '@material-ui/core/styles/withStyles';
import ThemeWrapper from 'components/ThemeWrapper';
import { CustomTableCell, styles } from 'components/PreviewData/DataView/Table';

const I18N_PREFIX = 'features.PreviewData.RecordView.RecordTable';

interface IRecordTableProps extends WithStyles<typeof styles> {
  headers: string[];
  record: any;
}

const RecordTableView: React.FC<IRecordTableProps> = ({ classes, headers, record }) => {
  // Used to stringify any non-string field values and field names.
  // TO DO: Might not need to do this for field names, need to test with nested schemas.
  // TO DO: Move to utilities, since we also use this in data view
  const format = (field: any) => {
    if (typeof field === 'object') {
      return JSON.stringify(field);
    }
    return field;
  };

  return (
    <Paper className={classes.root}>
      <Table>
        <TableHead>
          <TableRow className={classes.row}>
            <CustomTableCell>Field</CustomTableCell>
            <CustomTableCell>Value</CustomTableCell>
          </TableRow>
        </TableHead>
        <TableBody>
          {headers.map((fieldName, i) => {
            return (
              <TableRow className={classes.row} key={`tr-${i}}`}>
                <CustomTableCell>{format(fieldName)}</CustomTableCell>
                <CustomTableCell>{format(record[fieldName])}</CustomTableCell>
              </TableRow>
            );
          })}
        </TableBody>
      </Table>
    </Paper>
  );
};

const StyledRecordTable = withStyles(styles)(RecordTableView);

function RecordTable(props) {
  return (
    <ThemeWrapper>
      <StyledRecordTable {...props} />
    </ThemeWrapper>
  );
}

export default RecordTable;
