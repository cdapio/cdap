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

import withStyles, { StyleRules, WithStyles } from '@material-ui/core/styles/withStyles';
import classnames from 'classnames';
import { LEFT_PANEL_WIDTH } from 'components/HttpExecutor';
import HttpExecutorActions from 'components/HttpExecutor/store/HttpExecutorActions';
import HttpExecutorStore from 'components/HttpExecutor/store/HttpExecutorStore';
import { Map, OrderedMap } from 'immutable';
import * as React from 'react';
import { connect } from 'react-redux';

const styles = (theme): StyleRules => {
  return {
    root: {
      borderRight: `1px solid ${theme.palette.grey[300]}`,
      height: '100%',
    },
    historyRow: {
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
      width: `${LEFT_PANEL_WIDTH / 1.5}px`,
      wordWrap: 'break-word',
      textAlign: 'left',
      textTransform: 'lowercase',
      fontSize: '10px',
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

enum RequestMethod {
  GET = 'GET',
  POST = 'POST',
  PUT = 'PUT',
  DELETE = 'DELTE',
}

interface IRequestHistory {
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
  loading: boolean;
  activeTab: number;
  requestHistoryIsIncoming: boolean;
}

interface IRequestHistoryTabProps extends WithStyles<typeof styles> {
  requestHistory: Map<string, IRequestHistory>;
  requestHistoryIsIncoming: boolean;
  onHistoryClick: (timestamp: string, requestHistory: Map<string, IRequestHistory>) => void;
}

const mapStateToProps = (state) => {
  return {
    requestHistoryIsIncoming: state.http.requestHistoryIsIncoming,
  };
};

const mapDispatch = (dispatch) => {
  return {
    onHistoryClick: (timestamp: string, requestHistory: Map<string, IRequestHistory>) => {
      dispatch({
        type: HttpExecutorActions.setRequestHistoryView,
        payload: requestHistory.get(timestamp),
      });
    },
  };
};

const RequestHistoryTabView: React.FC<IRequestHistoryTabProps> = ({
  classes,
  requestHistoryIsIncoming,
  onHistoryClick,
}) => {
  const [requestHistory, setRequestHistory] = React.useState(OrderedMap<string, any>({}));

  // Query through localstorage and popluate RequestHistoryTab
  React.useEffect(() => {
    const sortedByTimestamp = Object.keys(localStorage).sort((a, b) => {
      const timestampA = new Date(a.substr(14));
      const timestampB = new Date(b.substr(14));
      if (timestampA < timestampB) {
        return 1;
      } else if (timestampA > timestampB) {
        return -1;
      } else {
        return 0;
      }
    });
    const newHistory = [];
    sortedByTimestamp.forEach((key) => {
      if (key.startsWith('RequestHistory')) {
        const history = JSON.parse(localStorage.getItem(key));
        newHistory.push([key, history]);
      }
    });
    setRequestHistory(OrderedMap(newHistory));
  }, []);

  // When new request history is incoming, update RequestHistoryTab
  React.useEffect(() => {
    if (!requestHistoryIsIncoming) {
      return;
    }
    const timestamp = `RequestHistory ${new Date().toLocaleString()}`;
    const history = HttpExecutorStore.getState().http;

    // Store new request history in local storage
    localStorage.setItem(timestamp, JSON.stringify(history));

    // Update the component view in real-time, since we cannot listen to local storage's change
    const newHistory = OrderedMap({ [timestamp]: history }).mergeWith(
      (history1, history2) => history1.concat(history2),
      requestHistory
    );
    setRequestHistory(newHistory);
  }, [requestHistoryIsIncoming]);

  return (
    <div className={classes.root}>
      {requestHistory.entrySeq().map(([timestamp, history]) => {
        return (
          <div
            key={timestamp}
            className={classes.historyRow}
            onClick={() => onHistoryClick(timestamp, requestHistory)}
          >
            <div
              className={classnames(classes.requestMethod, {
                [classes.getMethod]: history.method === 'GET',
                [classes.postMethod]: history.method === 'POST',
                [classes.deleteMethod]: history.method === 'DELETE',
                [classes.putMethod]: history.method === 'PUT',
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
// const RequestHistoryTab = withStyles(styles)(RequestHistoryTabView);
export default RequestHistoryTab;
