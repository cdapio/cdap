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

import TableBody from '@material-ui/core/TableBody';
import TableHead from '@material-ui/core/TableHead';
import TableRow from '@material-ui/core/TableRow';
import TableCell from '@material-ui/core/TableCell';

import Paper from '@material-ui/core/Paper';
import withStyles, { WithStyles, StyleRules } from '@material-ui/core/styles/withStyles';
import isEmpty from 'lodash/isEmpty';
import { PREVIEW_STATUS } from 'services/PreviewStatus';
import Heading, { HeadingTypes } from 'components/Heading';
import ThemeWrapper from 'components/ThemeWrapper';
import VirtualScroll from 'components/VirtualScroll';
import T from 'i18n-react';
import classnames from 'classnames';

const I18N_PREFIX = 'features.PreviewData.Table';

export const CustomTableCell = withStyles((theme) => ({
  head: {
    backgroundColor: theme.palette.grey['300'],
    color: theme.palette.common.white,
    padding: 10,
    fontSize: 14,
  },
  body: {
    padding: 10,
    fontSize: 14,
  },
}))(TableCell);

export const messageTextStyle = {
  fontSize: '1.3rem !important',
  margin: 'unset',
  padding: '10px 5px',
};
export const styles = (theme): StyleRules => ({
  root: {
    display: 'inline-block',
    height: 'auto',
    marginTop: theme.spacing(1),
    overflowX: 'auto',
    borderRadius: 0,
  },
  tableContainer: {
    width: 'fit-content',
    minWidth: '100%',
  },
  row: {
    height: 40,
    '&.oddRow': {
      backgroundColor: theme.palette.grey['600'],
    },
  },
  messageText: messageTextStyle,
  headerRow: {
    backgroundColor: theme.palette.grey['300'],
    fontWeight: 500,
    color: theme.palette.common.white,
    fontSize: 14,
  },
  cell: {
    textAlign: 'left',
    whiteSpace: 'nowrap',
    padding: '0px 10px',
    textOverflow: 'ellipsis',
    overflow: 'hidden',
    borderLeft: `1px solid ${theme.palette.grey['500']}`,
    '&:first-of-type': {
      borderLeft: 'none',
    },
  },
  indexCell: {
    width: '50px',
  },
  headerCell: {
    borderLeft: `1px solid ${theme.palette.grey['500']}`,
  },
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

  const renderHeader = (header: string[]) => {
    return (
      <TableHead>
        <TableRow className={classnames(classes.headerRow)}>
          <CustomTableCell className={classnames(classes.indexCell, classes.headerCell)} />
          {headers.map((fieldName, i) => {
            const processedFieldName = format(fieldName);
            return (
              <CustomTableCell key={`header-cell-${i}`} className={classes.headerCell}>
                {processedFieldName}
              </CustomTableCell>
            );
          })}
        </TableRow>
      </TableHead>
    );
  };

  const renderList = (visibleNodeCount: number, startNode: number) => {
    return records.slice(startNode, startNode + visibleNodeCount).map((record, j) => {
      const rowIndex = startNode + j + 1;
      return (
        <TableBody>
          <TableRow
            key={`table-row-${j}`}
            className={classnames(classes.row, { oddRow: rowIndex % 2 })}
          >
            <CustomTableCell
              className={classnames(classes.indexCell, classes.cell)}
              key={`index-cell-${j}`}
            >
              {rowIndex}
            </CustomTableCell>
            {headers.map((fieldName, k) => {
              const processedValue = format(record[fieldName]);
              return (
                <CustomTableCell key={`table-cell-${j}-${k}`} className={classes.cell}>
                  {processedValue}
                </CustomTableCell>
              );
            })}
          </TableRow>
        </TableBody>
      );
    });
  };

  if (isEmpty(records) || isCondition) {
    return (
      <div>
        <Heading type={HeadingTypes.h3} label={getStatusMsg()} className={classes.messageText} />
      </div>
    );
  }

  return (
    <Paper className={classnames(classes.root, classes.tableContainer)}>
      <VirtualScroll
        itemCount={() => records.length}
        visibleChildCount={25}
        childHeight={40}
        renderList={renderList}
        childrenUnderFold={10}
        headerEl={renderHeader(headers)}
      />
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
