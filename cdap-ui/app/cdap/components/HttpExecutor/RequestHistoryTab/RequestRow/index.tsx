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

import withStyles, { StyleRules, WithStyles } from '@material-ui/core/styles/withStyles';

import DeleteDialog from 'components/HttpExecutor/RequestHistoryTab/RequestActionDialogs/DeleteDialog';
import DeleteIcon from '@material-ui/icons/Delete';
import HttpExecutorActions from 'components/HttpExecutor/store/HttpExecutorActions';
import { IRequestHistory } from 'components/HttpExecutor/RequestHistoryTab';
import { RequestMethod } from 'components/HttpExecutor';
import ScheduleIcon from '@material-ui/icons/Schedule';
import Tooltip from '@material-ui/core/Tooltip';
import classnames from 'classnames';
import { connect } from 'react-redux';

const styles = (theme): StyleRules => {
  return {
    requestRow: {
      padding: '10px 10px',
      lineHeight: '24px',
      display: 'flex',
      justifyContent: 'space-between',
      width: '100%',
      cursor: 'pointer',

      '&:hover:not($selectedRequestRow)': {
        backgroundColor: theme.palette.grey[700],
        '& $requestActionButton': {
          visibility: 'visible',
        },
      },
    },
    selectedRequestRow: {
      backgroundColor: `${theme.palette.blue[500]} !important`,
    },
    requestMethod: {
      paddingLeft: '5px',
      paddingRight: '5px',
      color: theme.palette.white[50],
      height: '100%',
      fontWeight: 800,
      justifyContent: 'center',
      alignItems: 'flex-start',
      fontSize: '10px',
      width: '10%',
    },
    requestMethodText: {
      width: '100%',
      textAlign: 'left',
      alignSelf: 'center',
    },
    requestPath: {
      maxWidth: '70%',
      minWidth: '70%',
      wordWrap: 'break-word',
      textAlign: 'left',
      alignSelf: 'center',
      textTransform: 'lowercase',
      fontSize: '10px',
      display: 'inline-block',
      lineHeight: '1.3',
    },
    selectedRequestPath: {
      color: theme.palette.orange[50],
    },
    getMethod: {
      color: theme.palette.green[400],
    },
    postMethod: {
      color: theme.palette.yellow[50],
    },
    putMethod: {
      color: theme.palette.blue[50],
    },
    deleteMethod: {
      color: theme.palette.red[50],
    },
    buttonTooltip: {
      fontSize: '14px',
      backgroundColor: theme.palette.grey[50],
    },
    requestActionButtons: {
      justifyContent: 'flex-end',
      whiteSpace: 'nowrap',
    },
    requestActionButton: {
      display: 'inline-block',
      visibility: 'hidden',
    },
    successStatus: {
      color: theme.palette.green[50],
    },
    dangerStatus: {
      color: theme.palette.red[100],
    },
  };
};

const mapStateToProps = (state) => {
  return {
    selectedRequest: state.http.selectedRequest,
  };
};

const mapDispatch = (dispatch) => {
  return {
    setRequestHistoryView: (request: IRequestHistory) => {
      dispatch({
        type: HttpExecutorActions.setRequestHistoryView,
        payload: request,
      });
    },
  };
};

interface IRequestRowProps extends WithStyles<typeof styles> {
  request: IRequestHistory;
  selectedRequest: IRequestHistory;
  setRequestHistoryView: (request: IRequestHistory) => void;
}

const RequestRowView: React.FC<IRequestRowProps> = ({
  classes,
  request,
  selectedRequest,
  setRequestHistoryView,
}) => {
  const [deleteDialogOpen, setDeleteDialogOpen] = React.useState(false);

  const onRequestClick = (req: IRequestHistory) => {
    setRequestHistoryView(req);
  };

  const isSelectedRequest = selectedRequest && request.id === selectedRequest.id;

  return (
    <div>
      <div
        data-cy={`request-row-${request.id}`}
        className={classnames(classes.requestRow, {
          [classes.selectedRequestRow]: isSelectedRequest,
        })}
        onClick={() => onRequestClick(request)}
      >
        <div className={classes.requestTimestamp}>
          <Tooltip
            classes={{
              tooltip: classes.buttonTooltip,
            }}
            title={request.timestamp}
            placement="right"
            className={classes.requestActionButton}
            data-cy="timestamp-tooltip"
          >
            <ScheduleIcon data-cy="timestamp-icon" />
          </Tooltip>
        </div>
        <div
          className={classnames({
            [classes.successStatus]: request.statusCode < 300,
            [classes.dangerStatus]: request.statusCode !== null && request.statusCode >= 300,
          })}
        >
          {request.statusCode}
        </div>
        <div
          className={classnames(classes.requestMethod, {
            [classes.getMethod]: request.method === RequestMethod.GET,
            [classes.postMethod]: request.method === RequestMethod.POST,
            [classes.deleteMethod]: request.method === RequestMethod.DELETE,
            [classes.putMethod]: request.method === RequestMethod.PUT,
          })}
        >
          <div className={classes.requestMethodText} data-cy="request-method">
            {request.method}
          </div>
        </div>
        <div
          data-cy="request-path"
          className={classnames(classes.requestPath, {
            [classes.selectedRequestPath]: isSelectedRequest,
          })}
        >
          {request.path}
        </div>
        <div className={classes.requestActionButtons}>
          <Tooltip
            classes={{
              tooltip: classes.buttonTooltip,
            }}
            title={'Delete request'}
            placement="bottom"
            className={classes.requestActionButton}
          >
            <DeleteIcon data-cy="delete-icon" onClick={() => setDeleteDialogOpen(true)} />
          </Tooltip>
        </div>
      </div>
      <DeleteDialog
        request={request}
        open={deleteDialogOpen}
        handleClose={() => setDeleteDialogOpen(false)}
      />
    </div>
  );
};

const RequestRow = withStyles(styles)(connect(mapStateToProps, mapDispatch)(RequestRowView));
export default RequestRow;
