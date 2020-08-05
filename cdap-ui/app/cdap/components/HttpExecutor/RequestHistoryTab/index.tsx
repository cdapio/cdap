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
import {
  compareByTimestamp,
  getDateID,
  getRequestsByDate,
} from 'components/HttpExecutor/utilities';
import withStyles, { StyleRules, WithStyles } from '@material-ui/core/styles/withStyles';

import Button from '@material-ui/core/Button';
import ClearAllDialog from 'components/HttpExecutor/RequestHistoryTab/RequestActionDialogs/ClearAllDialog';
import ExpandMoreIcon from '@material-ui/icons/ExpandMore';
import ExpansionPanel from '@material-ui/core/ExpansionPanel';
import ExpansionPanelDetails from '@material-ui/core/ExpansionPanelDetails';
import ExpansionPanelSummary from '@material-ui/core/ExpansionPanelSummary';
import HttpExecutorActions from 'components/HttpExecutor/store/HttpExecutorActions';
import If from 'components/If';
import { RequestMethod } from 'components/HttpExecutor';
import RequestRow from 'components/HttpExecutor/RequestHistoryTab/RequestRow';
import RequestSearch from 'components/HttpExecutor/RequestHistoryTab/RequestSearch';
import Typography from '@material-ui/core/Typography';
import { connect } from 'react-redux';

export const REQUEST_HISTORY = 'RequestHistory';

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
  };
};

const styles = (theme): StyleRules => {
  return {
    root: {
      borderRight: `1px solid ${theme.palette.grey[300]}`,
      height: '100%',
    },
    introRow: {
      display: 'grid',
      width: '100%',
      gridTemplateColumns: 'repeat(7, 1fr)',
      padding: `${theme.spacing(1)}px 0`,
    },
    title: {
      fontSize: '15px',
      paddingLeft: `${theme.Spacing(3)}px`,
      gridColumnStart: '1',
      width: '100px',
      overflow: 'hidden',
      whiteSpace: 'nowrap',
      display: 'flex',
      alignItems: 'center',
      justifyContent: 'center',
    },
    clearAllBtn: {
      width: '100%',
      gridColumnStart: '7',
      display: 'flex',
      alignItems: 'center',
      justifyContent: 'center',
      textTransform: 'none',
    },
    timestampGroup: {
      display: 'flex',
      flexFlow: 'column',
      padding: '0',
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

const StyledExpansionPanelSummary = withStyles({
  expandIcon: {
    order: -1,
    paddingRight: '10px',
  },
  root: {
    height: '35px',
    padding: '0px',
  },
})(ExpansionPanelSummary);

interface IRequestHistoryTabProps extends WithStyles<typeof styles> {
  requestLog: Map<string, List<IRequestHistory>>;
  setRequestLog: (requestLog: Map<string, List<IRequestHistory>>) => void;
}

const RequestHistoryTabView: React.FC<IRequestHistoryTabProps> = ({
  classes,
  requestLog,
  setRequestLog,
}) => {
  const [ClearAllDialogOpen, setClearAllDialogOpen] = React.useState(false);
  const [searchText, setSearchText] = React.useState('');

  // Query through localstorage to populate RequestHistoryTab
  // requestLog maps timestamp date (e.g. April 5th) to a list of corresponding request histories, sorted by timestamp
  React.useEffect(() => {
    fetchFromLocalStorage();
  }, []);

  const fetchFromLocalStorage = () => {
    // Group and sort logs by timestamp
    let newRequestLog = Map<string, List<IRequestHistory>>({});

    const storedLogs = localStorage.getItem(REQUEST_HISTORY);
    if (storedLogs) {
      try {
        const savedCalls = List(JSON.parse(storedLogs));
        savedCalls
          .sort((a: IRequestHistory, b: IRequestHistory) =>
            compareByTimestamp(a.timestamp, b.timestamp)
          )
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
  };

  const getFilteredRequestLogs = (requests: List<IRequestHistory>, query: string) => {
    return requests.filter((request) => request.path.includes(query));
  };

  return (
    <div className={classes.root} data-cy="request-history-tab">
      <div className={classes.introRow}>
        <div className={classes.title}>Calls history</div>
        <Button
          color="primary"
          onClick={() => setClearAllDialogOpen(true)}
          className={classes.clearAllBtn}
          data-cy="clear-btn"
        >
          Clear
        </Button>
      </div>
      <RequestSearch searchText={searchText} setSearchText={setSearchText} />
      {requestLog
        .keySeq()
        .toArray()
        .sort((a: string, b: string) => compareByTimestamp(a, b))
        .map((dateID: string) => {
          const requests: List<IRequestHistory> = requestLog.get(dateID);
          const filteredRequests = getFilteredRequestLogs(requests, searchText);
          return (
            <div key={dateID}>
              <If condition={filteredRequests.size > 0}>
                <StyledExpansionPanel key={dateID} defaultExpanded elevation={0}>
                  <StyledExpansionPanelSummary expandIcon={<ExpandMoreIcon />}>
                    <Typography>{dateID}</Typography>
                  </StyledExpansionPanelSummary>
                  <ExpansionPanelDetails className={classes.timestampGroup}>
                    {filteredRequests.map((request, requestIndex) => (
                      <RequestRow key={requestIndex} request={request} />
                    ))}
                  </ExpansionPanelDetails>
                </StyledExpansionPanel>
              </If>
            </div>
          );
        })}
      <ClearAllDialog open={ClearAllDialogOpen} handleClose={() => setClearAllDialogOpen(false)} />
    </div>
  );
};

const RequestHistoryTab = withStyles(styles)(
  connect(mapStateToProps, mapDispatch)(RequestHistoryTabView)
);
export default RequestHistoryTab;
