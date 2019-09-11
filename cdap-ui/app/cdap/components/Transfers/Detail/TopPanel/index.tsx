/*
 * Copyright © 2019 Cask Data, Inc.
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
import withStyles, { WithStyles, StyleRules } from '@material-ui/core/styles/withStyles';
import { transfersDetailConnect } from 'components/Transfers/Detail/context';
import StatusIndicator from 'components/StatusIndicator';
import PlayArrow from '@material-ui/icons/PlayArrow';
import Stop from '@material-ui/icons/Stop';
import Description from '@material-ui/icons/Description';
import Delete from '@material-ui/icons/Delete';
import IconSVG from 'components/IconSVG';
import If from 'components/If';
import { getCurrentNamespace } from 'services/NamespaceStore';
import moment from 'moment';
import { start, stop, deleteApp } from 'components/Transfers/utilities';
import { Redirect } from 'react-router';

const styles = (theme): StyleRules => {
  return {
    root: {
      height: '75px',
      padding: '0 25px',
      backgroundColor: theme.palette.grey[600],
      display: 'grid',
      gridTemplateColumns: '33% 33% 33%',
      alignItems: 'center',
    },
    title: {
      marginRight: '15px',
      marginBottom: 0,
    },
    titleSection: {
      display: 'flex',
      alignItems: 'center',
    },
    button: {
      height: '50px',
      width: '60px',
      borderWidth: '2px',
      borderRadius: '4px',
      padding: 0,
      '&:hover': {
        backgroundColor: `${theme.palette.grey[700]} !important`,
      },
    },
    btnIcon: {
      fontSize: '2.5rem',
    },
    btnText: {
      marginTop: '-5px',
    },
    start: {
      color: theme.palette.blue[100],
    },
    stop: {
      color: theme.palette.red[100],
    },
    loading: {
      color: theme.palette.grey[300],
      fontSize: '2.0rem !important',
      padding: '3px',
    },
    logs: {
      height: '50px',
      width: '70px',
      marginLeft: '10px',
      borderLeft: `2px solid ${theme.palette.grey[400]}`,
      borderRight: `2px solid ${theme.palette.grey[400]}`,
      color: theme.palette.grey[50],
    },
    logsBtn: {
      fontSize: '2rem',
    },
    logsText: {
      marginTop: '-2px',
    },
    deleteBtn: {
      color: theme.palette.grey[100],
      '&:hover, &:focus': {
        textDecoration: 'none',
      },
    },
  };
};

interface ITopPanel extends WithStyles<typeof styles> {
  name: string;
  status: string;
  description: string;
  id: string;
  fetchStatus: () => void;
}

const TopPanelView: React.SFC<ITopPanel> = ({
  classes,
  name,
  status,
  description,
  id,
  fetchStatus,
}) => {
  const [btnLoading, setBtnLoading] = React.useState(false);
  const [redirect, setRedirect] = React.useState(false);

  const startBtn = <PlayArrow className={`${classes.btnIcon} ${classes.start}`} />;
  const stopBtn = <Stop className={`${classes.btnIcon} ${classes.stop}`} />;
  const loading = (
    <span className={`fa fa-spin ${classes.loading} ${classes.btnIcon}`}>
      <IconSVG name="icon-spinner" />
    </span>
  );
  const disabled = ['STARTING', 'STOPPING'].indexOf(status) !== -1;

  let btnText = 'Start';
  if (status === 'RUNNING') {
    btnText = 'Stop';
  } else if (disabled || btnLoading) {
    btnText = 'Loading';
  }

  const appName = `CDC-${id}`;

  const startTime = moment()
    .subtract(7, 'days')
    .format('X');
  let logUrl = `/v3/namespaces/${getCurrentNamespace()}/apps/${appName}/workers/DeltaWorker/logs`;

  logUrl = `${logUrl}?start=${startTime}`;
  logUrl = `/downloadLogs?type=raw&backendPath=${encodeURIComponent(logUrl)}`;

  React.useEffect(
    () => {
      setBtnLoading(false);
    },
    [status]
  );

  function handleClick() {
    setBtnLoading(true);
    if (status !== 'RUNNING') {
      start(appName, fetchStatus, fetchStatus);
    } else {
      stop(appName, fetchStatus, fetchStatus);
    }
  }

  function handleDelete() {
    deleteApp(
      { id },
      () => {
        setRedirect(true);
      },
      null
    );
  }

  return (
    <div className={classes.root}>
      <div className={classes.titleSection}>
        <h1 title={description} className={classes.title}>
          {name}
        </h1>
        <StatusIndicator status={status} />
      </div>
      <div className="text-center">
        <button
          className={`btn btn-secondary ${classes.button}`}
          disabled={disabled || btnLoading}
          onClick={handleClick}
        >
          <If condition={status === 'STOPPED'}>{startBtn}</If>
          <If condition={disabled}>{loading}</If>
          <If condition={status === 'RUNNING'}>{stopBtn}</If>
          <div className={classes.btnText}>{btnText}</div>
        </button>

        <a href={logUrl} target="_tab" className={`${classes.logs} btn`}>
          <Description className={classes.logsBtn} />
          <div className={classes.logsText}>Logs</div>
        </a>
      </div>
      <div className="text-right">
        <button className={`${classes.deleteBtn} btn btn-link`} onClick={handleDelete}>
          <Delete className={classes.logsBtn} />
          <div>Delete</div>
        </button>
      </div>

      {redirect ? <Redirect to={`/ns/${getCurrentNamespace()}/transfers`} /> : null}
    </div>
  );
};

const StyledTopPanel = withStyles(styles)(TopPanelView);
const TopPanel = transfersDetailConnect(StyledTopPanel);
export default TopPanel;
