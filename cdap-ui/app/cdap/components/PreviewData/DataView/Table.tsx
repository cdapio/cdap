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
import TableHead from '@material-ui/core/TableHead';
import TableRow from '@material-ui/core/TableRow';
import Paper from '@material-ui/core/Paper';
import TableCell from '@material-ui/core/TableCell';
import withStyles, { WithStyles } from '@material-ui/core/styles/withStyles';
import isEmpty from 'lodash/isEmpty';
import { PREVIEW_STATUS } from 'services/PreviewStatus';
import Heading, { HeadingTypes } from 'components/Heading';
import ThemeWrapper from 'components/ThemeWrapper';
import T from 'i18n-react';

const I18N_PREFIX = 'features.PreviewData.DataView.Table';

export const CustomTableCell = withStyles((theme) => ({
  head: {
    backgroundColor: theme.palette.grey['300'],
    color: theme.palette.common.white,
    padding: 10,
    fontSize: 14,
    '&:first-of-type': {
      borderRight: `1px solid ${theme.palette.grey['500']}`,
    },
  },
  body: {
    padding: 10,
    fontSize: 14,
    '&:first-of-type': {
      borderRight: `1px solid ${theme.palette.grey['500']}`,
    },
  },
}))(TableCell);

export const messageTextStyle = {
  fontSize: '1.3rem !important',
  margin: '10px 0',
};
export const styles = (theme) => ({
  root: {
    width: '100%',
    display: 'inline-block',
    height: 'auto',
    marginTop: theme.spacing(1),
  },
  table: {
    width: '100%',
  },
  row: {
    height: 40,
    '&:nth-of-type(odd)': {
      backgroundColor: theme.palette.grey['600'],
    },
  },
  messageText: messageTextStyle,
});

interface IDataTableProps extends WithStyles<typeof styles> {
  headers?: string[];
  records?: any[];
  isInput?: boolean;
  previewStatus?: string;
  isCondition?: boolean;
}

const DataTableView: React.FC<IDataTableProps> = ({
  classes,
  headers,
  records,
  isInput,
  previewStatus,
  isCondition,
}) => {
  const getStatusMsg = () => {
    let msg;
    const recordType = isInput ? 'Input' : 'Output';
    if (isCondition) {
      msg = T.translate(`${I18N_PREFIX}.previewNotSupported`);
    } else if (previewStatus === PREVIEW_STATUS.RUNNING || previewStatus === PREVIEW_STATUS.INIT) {
      // preview is still running but there's no data yet
      msg = T.translate(`${I18N_PREFIX}.previewRunning`, { recordType });
    } else {
      // not running preview but there is no preview data
      msg = T.translate(`${I18N_PREFIX}.noPreviewRunning`, { recordType });
    }
    return msg;
  };

  // Used to stringify any non-string field values and field names.
  // TO DO: Might not need to do this for field names, need to test with nested schemas
  const format = (field: any) => {
    if (typeof field === 'object') {
      return JSON.stringify(field);
    }
    return field;
  };

  if (isEmpty(records) || isCondition) {
    return (
      <div>
        <Heading type={HeadingTypes.h3} label={getStatusMsg()} className={classes.messageText} />
      </div>
    );
  }

  return (
    <Paper className={classes.root}>
      <Table>
        <TableHead>
          <TableRow className={classes.row}>
            <CustomTableCell />
            {headers.map((fieldName, i) => {
              return (
                <CustomTableCell key={`header-cell-${i}`}>{format(fieldName)}</CustomTableCell>
              );
            })}
          </TableRow>
        </TableHead>
        <TableBody>
          {records.map((record, j) => {
            return (
              <TableRow className={classes.row} key={`tr-${j}`}>
                <CustomTableCell>{j + 1}</CustomTableCell>
                {headers.map((fieldName, k) => {
                  return (
                    <CustomTableCell key={`table-cell-${k}`}>
                      {format(record[fieldName])}
                    </CustomTableCell>
                  );
                })}
              </TableRow>
            );
          })}
        </TableBody>
      </Table>
    </Paper>
  );
};

const StyledDataTable = withStyles(styles)(DataTableView);

function DataTable(props) {
  return (
    <ThemeWrapper>
      <StyledDataTable {...props} />
    </ThemeWrapper>
  );
}

export default DataTable;
