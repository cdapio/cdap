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
import VirtualScroll from 'components/VirtualScroll';
import { PREVIEW_STATUS } from 'services/PreviewStatus';
import Paper from '@material-ui/core/Paper';
import Grid from '@material-ui/core/Grid';
import withStyles, { WithStyles, StyleRules } from '@material-ui/core/styles/withStyles';
import ThemeWrapper from 'components/ThemeWrapper';
import { styles as tableStyles } from 'components/PreviewData/DataView/Table';
import classnames from 'classnames';
import T from 'i18n-react';
import Heading, { HeadingTypes } from 'components/Heading';

const I18N_PREFIX = 'features.PreviewData.RecordView.RecordTable';

// Info about rows in record table
// Max number of visible rows
const visibleChildCount = 25;
// Height of row in px
const childHeight = 40;
// number of rows in dom but not in viewport
const childrenUnderFold = 10;

const styles = (theme): StyleRules => ({
  ...tableStyles(theme),
  recordCell: {
    width: '50%',
    '&:first-of-type': {
      borderRight: `1px solid ${theme.palette.grey['500']}`,
      fontWeight: 500,
    },
  },
  recordContainer: {
    width: '100%',
    padding: '10px',
    marginTop: 'unset',
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

  const renderList = (visibleNodeCount: number, startNode: number) => {
    if (!record) {
      return;
    }
    return headers.slice(startNode, startNode + visibleNodeCount).map((fieldName, i) => {
      const processedFieldName = format(fieldName);
      const processedValue = format(record[fieldName]);
      return (
        <React.Fragment>
          <Grid
            container
            direction="row"
            wrap="nowrap"
            className={classnames(classes.row, { oddRow: (i + startNode + 1) % 2 })}
            key={`gridrow-${i}`}
          >
            <Grid
              item
              className={classnames(classes.cell, classes.recordCell)}
              title={processedFieldName}
              data-cy={`fieldname-${processedFieldName}`}
            >
              {processedFieldName}
            </Grid>
            <Grid
              item
              className={classnames(classes.cell, classes.recordCell)}
              title={processedValue}
              data-cy={`value-${processedValue}`}
            >
              {processedValue}
            </Grid>
          </Grid>
        </React.Fragment>
      );
    });
  };

  // When the selected record number is out of range
  if (!record) {
    return noRecordMsg();
  }

  return (
    <Paper className={classnames(classes.root, classes.recordContainer)}>
      <Grid container direction="column" wrap="nowrap">
        <Grid item>
          <Grid
            container
            direction="row"
            justify="center"
            alignItems="center"
            className={classes.headerRow}
          >
            <Grid item className={classnames(classes.cell, classes.recordCell)}>
              {T.translate(`${I18N_PREFIX}.fieldName`)}
            </Grid>
            <Grid item className={classnames(classes.cell, classes.recordCell)}>
              {T.translate(`${I18N_PREFIX}.value`)}
            </Grid>
          </Grid>
        </Grid>
        <Grid item>
          <VirtualScroll
            itemCount={() => headers.length}
            visibleChildCount={visibleChildCount}
            childHeight={childHeight}
            renderList={renderList}
            childrenUnderFold={childrenUnderFold}
          />
        </Grid>
      </Grid>
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
