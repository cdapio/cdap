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

import { List, Map } from 'immutable';
import { getDateID, getRequestsByDate } from 'components/HttpExecutor/utilities';
import withStyles, { StyleRules, WithStyles } from '@material-ui/core/styles/withStyles';

import ExpansionPanel from '@material-ui/core/ExpansionPanel';
import ExpansionPanelActions from '@material-ui/core/ExpansionPanelActions';
import ExpansionPanelSummary from '@material-ui/core/ExpansionPanelSummary';
import HttpExecutorActions from 'components/HttpExecutor/store/HttpExecutorActions';
import { REQUEST_HISTORY } from 'components/HttpExecutor/store/HttpExecutorStore';
import { RequestMethod } from 'components/HttpExecutor';
import Typography from '@material-ui/core/Typography';
import classnames from 'classnames';
import { connect } from 'react-redux';

export interface IRequestHistory {
  timestamp: string;
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
    timestampGroup: {
      display: 'flex',
      flexFlow: 'column',
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

const StyledExpansionPanel = withStyles((theme) => ({
  root: {
    '&$expanded': {
      margin: 0,
    },
    borderBottom: `1px solid ${theme.palette.grey[300]}`,
  },
  /* Styles applied to the root element if `expanded={true}`. */
  expanded: {},
}))(ExpansionPanel);

interface IRequestHistoryTabProps extends WithStyles<typeof styles> {
  requestLog: Map<string, List<IRequestHistory>>;
  setRequestLog: (requestLog: Map<string, List<IRequestHistory>>) => void;
  onRequestClick: (request: IRequestHistory) => void;
}

const mapStateToProps = (state) => {
  return {
    requestLog: state.http.requestLog,
  };
};

const mapDispatch = (dispatch) => {
  return {
    setRequestLog: (requestLog: Map<string, List<IRequestHistory>>) => {
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
  // requestLog maps timestamp date (e.g. April 5th) to a list of corresponding request histories, sorted by timestamp
  React.useEffect(() => {
    // Group and sort logs by timestamp
    let newRequestLog = Map<string, List<IRequestHistory>>({});

    const storedLogs = localStorage.getItem(REQUEST_HISTORY);
    if (storedLogs) {
      try {
        const savedCalls = List(JSON.parse(storedLogs));
        savedCalls
          .sort((a: IRequestHistory, b: IRequestHistory) => {
            const timestampA = new Date(a.timestamp);
            const timestampB = new Date(b.timestamp);
            if (timestampA < timestampB) {
              return 1;
            } else if (timestampA > timestampB) {
              return -1;
            } else {
              return 0;
            }
          })
          .forEach((req: IRequestHistory) => {
            const timestamp = new Date(req.timestamp);
            const dateID: string = getDateID(timestamp);
            const requestsGroup = getRequestsByDate(newRequestLog, dateID);
            newRequestLog = newRequestLog.set(dateID, requestsGroup.push(req));
          });
        setRequestLog(newRequestLog);
      } catch (e) {
        setRequestLog(Map({}));
      }
    } else {
      // If REQUEST_HISTORY key doesn't exist in localStorage, initialize it
      localStorage.setItem(REQUEST_HISTORY, JSON.stringify([]));
      setRequestLog(Map({}));
    }
  }, []);

  return (
    <div className={classes.root}>
      {requestLog.keySeq().map((timestamp) => {
        const requestHistories = requestLog.get(timestamp);
        return (
          <StyledExpansionPanel key={timestamp} defaultExpanded elevation={0}>
            <ExpansionPanelSummary classes={{ root: classes.root, expanded: classes.expanded }}>
              <Typography>{timestamp}</Typography>
            </ExpansionPanelSummary>
            <ExpansionPanelActions className={classes.timestampGroup}>
              {requestHistories.map((request, requestIndex) => {
                return (
                  <div
                    key={`request-${requestIndex}`}
                    className={classes.requestRow}
                    onClick={() => onRequestClick(request)}
                  >
                    <div
                      className={classnames(classes.requestMethod, {
                        [classes.getMethod]: request.method === RequestMethod.GET,
                        [classes.postMethod]: request.method === RequestMethod.POST,
                        [classes.deleteMethod]: request.method === RequestMethod.DELETE,
                        [classes.putMethod]: request.method === RequestMethod.PUT,
                      })}
                    >
                      <div className={classes.requestMethodText}>{request.method}</div>
                    </div>
                    <div className={classes.requestPath}>{request.path}</div>
                  </div>
                );
              })}
            </ExpansionPanelActions>
          </StyledExpansionPanel>
        );
      })}
    </div>
  );
};

const RequestHistoryTab = withStyles(styles)(
  connect(mapStateToProps, mapDispatch)(RequestHistoryTabView)
);
export default RequestHistoryTab;
