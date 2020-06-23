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
import Paper from '@material-ui/core/Paper';
import Grid from '@material-ui/core/Grid';
import withStyles, { WithStyles, StyleRules } from '@material-ui/core/styles/withStyles';
import isEmpty from 'lodash/isEmpty';
import { PREVIEW_STATUS } from 'services/PreviewStatus';
import Heading, { HeadingTypes } from 'components/Heading';
import ThemeWrapper from 'components/ThemeWrapper';
import VirtualScroll from 'components/VirtualScroll';
import T from 'i18n-react';
import classnames from 'classnames';

const I18N_PREFIX = 'features.PreviewData.DataView.Table';

export const messageTextStyle = {
  fontSize: '1.3rem !important',
  margin: '10px 0',
};
export const styles = (theme): StyleRules => ({
  root: {
    width: 'fit-content',
    minWidth: '100%',
    display: 'inline-block',
    height: 'auto',
    marginTop: theme.spacing(1),
  },
  table: {
    width: '100%',
  },
  row: {
    height: 40,
    '&.oddRow': {
      backgroundColor: theme.palette.grey['600'],
    },
  },
  messageText: messageTextStyle,
  headerCell: {
    backgroundColor: theme.palette.grey['300'],
    color: theme.palette.common.white,
    fontSize: 14,
    '&.indexCell': {
      width: '40px',
    },
  },
  cell: {
    width: '100px',
    textAlign: 'center',
    height: '40px',
    overflow: 'hidden',
    textOverflow: 'ellipsis',
    '&:first-of-type': {
      borderRight: `1px solid ${theme.palette.grey['500']}`,
    },
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
  const renderList = (visibleNodeCount: number, startNode: number) => {
    return records.slice(startNode, startNode + visibleNodeCount).map((record, i) => {
      return (
        <React.Fragment>
          <Grid
            container
            direction="row"
            wrap="nowrap"
            className={classnames(classes.row, { oddRow: (i + startNode + 1) % 2 })}
            key={`tr-${i}`}
          >
            <Grid item className={classes.cell}>
              {i + 1 + startNode}
            </Grid>
            {headers.map((fieldName, k) => {
              return (
                <Grid item className={classes.cell} key={`table-cell-${k}`}>
                  {format(record[fieldName])}
                </Grid>
              );
            })}
          </Grid>
        </React.Fragment>
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
    <Paper className={classes.root}>
      <Grid container direction="column" wrap="nowrap">
        <Grid item>
          <Grid container direction="row" justify="center" alignItems="center">
            <Grid
              item
              className={classnames(classes.headerCell, classes.cell, classes.indexCell)}
            />
            {headers.map((fieldName, i) => {
              return (
                <Grid
                  item
                  key={`header-cell-${i}`}
                  className={classnames(classes.cell, classes.headerCell)}
                >
                  {format(fieldName)}
                </Grid>
              );
            })}
          </Grid>
        </Grid>
        <Grid item>
          <VirtualScroll
            itemCount={() => records.length}
            visibleChildCount={25}
            childHeight={40}
            renderList={renderList}
            childrenUnderFold={10}
          />
        </Grid>
      </Grid>
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
