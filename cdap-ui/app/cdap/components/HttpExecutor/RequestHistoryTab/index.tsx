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

import HttpExecutorActions from 'components/HttpExecutor/store/HttpExecutorActions';
import { List } from 'immutable';
import { REQUEST_HISTORY } from 'components/HttpExecutor/store/HttpExecutorStore';
import { RequestMethod } from 'components/HttpExecutor';
import classnames from 'classnames';
import { connect } from 'react-redux';

export interface IRequestHistory {
  method: RequestMethod;
  path: string;
  body: string;
  headers: {
    pairs: [
      {
        key: string;
        value: string;
        uniqueId: string;
      }
    ];
  };
  response: string;
  statusCode: number;
}

const styles = (theme): StyleRules => {
  return {
    root: {
      borderRight: `1px solid ${theme.palette.grey[300]}`,
      height: '100%',
    },
    requestRow: {
      padding: '10px',
      lineHeight: '24px',
      display: 'grid',
      width: '100%',
      gridTemplateColumns: '50px 1fr',
      cursor: 'pointer',

      '&:hover': {
        backgroundColor: theme.palette.grey[700],
      },
    },
    requestMethod: {
      paddingLeft: '5px',
      color: theme.palette.white[50],
      width: '100%',
      height: '100%',
      fontWeight: 600,
      display: 'flex',
      justifyContent: 'center',
      alignItems: 'flex-start',
      fontSize: '10px',
    },
    requestMethodText: {
      width: '100%',
      textAlign: 'left',
      alignSelf: 'center',
    },
    requestPath: {
      maxWidth: '80%',
      minWidth: '80%',
      wordWrap: 'break-word',
      textAlign: 'left',
      alignSelf: 'center',
      textTransform: 'lowercase',
      fontSize: '10px',
      display: 'inline-block',
      lineHeight: '1.3',
    },
    getMethod: {
      color: theme.palette.green[50],
    },
    postMethod: {
      color: theme.palette.orange[50],
    },
    putMethod: {
      color: theme.palette.yellow[50],
    },
    deleteMethod: {
      color: theme.palette.red[50],
    },
  };
};

interface IRequestHistoryTabProps extends WithStyles<typeof styles> {
  requestLog: List<IRequestHistory>;
  setRequestLog: (requestLog: List<IRequestHistory>) => void;
  onRequestClick: (request: IRequestHistory) => void;
}

const mapStateToProps = (state) => {
  return {
    requestLog: state.http.requestLog,
  };
};

const mapDispatch = (dispatch) => {
  return {
    setRequestLog: (requestLog: List<IRequestHistory>) => {
      dispatch({
        type: HttpExecutorActions.setRequestLog,
        payload: {
          requestLog,
        },
      });
    },
    onRequestClick: (request: IRequestHistory) => {
      dispatch({
        type: HttpExecutorActions.setRequestHistoryView,
        payload: request,
      });
    },
  };
};

const RequestHistoryTabView: React.FC<IRequestHistoryTabProps> = ({
  classes,
  requestLog,
  setRequestLog,
  onRequestClick,
}) => {
  // Query through localstorage to populate RequestHistoryTab
  React.useEffect(() => {
    const storedLogs = localStorage.getItem(REQUEST_HISTORY);
    if (storedLogs) {
      try {
        setRequestLog(List(JSON.parse(storedLogs)));
      } catch (e) {
        setRequestLog(List([]));
      }
    } else {
      localStorage.setItem(REQUEST_HISTORY, JSON.stringify([]));
      setRequestLog(List([]));
    }
  }, []);

  return (
    <div className={classes.root}>
      {requestLog.map((history, i) => {
        return (
          <div key={i} className={classes.requestRow} onClick={() => onRequestClick(history)}>
            <div
              className={classnames(classes.requestMethod, {
                [classes.getMethod]: history.method === RequestMethod.GET,
                [classes.postMethod]: history.method === RequestMethod.POST,
                [classes.deleteMethod]: history.method === RequestMethod.DELETE,
                [classes.putMethod]: history.method === RequestMethod.PUT,
              })}
            >
              <div className={classes.requestMethodText}>{history.method}</div>
            </div>
            <div className={classes.requestPath}>{history.path}</div>
          </div>
        );
      })}
    </div>
  );
};

const RequestHistoryTab = withStyles(styles)(
  connect(mapStateToProps, mapDispatch)(RequestHistoryTabView)
);
export default RequestHistoryTab;
