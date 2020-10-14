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
import { PREVIEW_STATUS } from 'services/PreviewStatus';
import Table from '@material-ui/core/Table';
import TableBody from '@material-ui/core/TableBody';
import TableHead from '@material-ui/core/TableHead';
import TableRow from '@material-ui/core/TableRow';
import withStyles, { WithStyles, StyleRules } from '@material-ui/core/styles/withStyles';
import ThemeWrapper from 'components/ThemeWrapper';
import { CustomTableCell, styles as tableStyles } from 'components/PreviewData/DataView/Table';
import classnames from 'classnames';
import T from 'i18n-react';
import Heading, { HeadingTypes } from 'components/Heading';

const I18N_PREFIX = 'features.PreviewData.RecordView.RecordTable';

const styles = (theme): StyleRules => ({
  ...tableStyles(theme),
  recordCell: {
    minWidth: '50%',
    overflow: 'hidden',
    textOverflow: 'ellipsis',
    whiteSpace: 'nowrap',
    borderLeft: `1px solid ${theme.palette.grey['500']}`,
    '&:first-of-type': {
      fontWeight: 500,
      borderLeft: 'none',
    },
  },
  recordRow: {
    '&:nth-of-type(odd)': {
      backgroundColor: theme.palette.grey['600'],
    },
  },
  recordContainer: {
    width: '100%',
    padding: '10px',
    overflowX: 'auto',
  },
  table: {
    backgroundColor: theme.palette.white['50'],
  },
});

interface IRecordTableProps extends WithStyles<typeof styles> {
  headers?: string[];
  record?: any;
  selectedRecord?: number;
  isInput?: boolean;
  previewStatus?: string;
  isCondition?: boolean;
}

const RecordTableView: React.FC<IRecordTableProps> = ({
  classes,
  headers,
  record,
  selectedRecord,
  isInput,
  previewStatus,
  isCondition,
}) => {
  // Used to stringify any non-string field values and field names.
  // TO DO: Might not need to do this for field names, need to test with nested schemas.
  // TO DO: Move to utilities, since we also use this in data view

  const format = (field: any) => {
    if (typeof field === 'object') {
      return JSON.stringify(field);
    }
    return field;
  };

  const noRecordMsg = () => {
    let msg;
    const recordType = isInput ? 'Input' : 'Output';
    if (isCondition) {
      msg = T.translate(`${I18N_PREFIX}.previewNotSupported`);
    } else if (previewStatus === PREVIEW_STATUS.RUNNING || previewStatus === PREVIEW_STATUS.INIT) {
      // preview is still running but there's no data yet
      msg = T.translate(`${I18N_PREFIX}.previewRunning`, { recordType });
    } else if (headers.length > 0) {
      // not running preview and there is preview data, but not for this record number
      msg = T.translate(`${I18N_PREFIX}.noSelectedRecord`, { selectedRecord });
    } else {
      // not running preview AND there is no preview data for this stage
      msg = T.translate(`${I18N_PREFIX}.noPreviewRunning`, { recordType });
    }
    return (
      <div>
        <Heading type={HeadingTypes.h3} label={msg} className={classes.messageText} />
      </div>
    );
  };

  // When the selected record number is out of range
  if (!record) {
    return noRecordMsg();
  }

  return (
    <div className={classnames(classes.root, classes.recordContainer)}>
      <Table className={classes.table}>
        <TableHead>
          <TableRow className={classes.row}>
            <CustomTableCell className={classes.recordCell}>
              {T.translate(`${I18N_PREFIX}.fieldName`)}
            </CustomTableCell>
            <CustomTableCell className={classes.recordCell}>
              {T.translate(`${I18N_PREFIX}.value`)}
            </CustomTableCell>
          </TableRow>
        </TableHead>
        <TableBody>
          {headers.map((fieldName, i) => {
            return (
              <TableRow className={classnames(classes.row, classes.recordRow)} key={`tr-${i}`}>
                <CustomTableCell className={classes.recordCell}>
                  {format(fieldName)}
                </CustomTableCell>
                <CustomTableCell className={classes.recordCell}>
                  {format(record[fieldName])}
                </CustomTableCell>
              </TableRow>
            );
          })}
        </TableBody>
      </Table>
    </div>
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
