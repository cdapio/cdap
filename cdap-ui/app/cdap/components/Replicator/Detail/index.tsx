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
import withStyles, { WithStyles, StyleRules } from '@material-ui/core/styles/withStyles';
import Button from '@material-ui/core/Button';
import { getCurrentNamespace } from 'services/NamespaceStore';
import { MyReplicatorApi } from 'api/replicator';
import { Map, fromJS } from 'immutable';
import If from 'components/If';
import { Link, Redirect } from 'react-router-dom';
import moment from 'moment';
import Status from 'components/Status';

const styles = (theme): StyleRules => {
  return {
    root: {
      padding: '25px 40px',
    },
    buttonContainer: {
      '& > *': {
        marginRight: '15px',
      },
    },
    config: {
      border: `1px solid ${theme.palette.grey[300]}`,
      borderRadius: '4px',
      '& > pre': {
        wordBreak: 'break-word',
        whiteSpace: 'pre-wrap',
        padding: '15px',
      },
    },
  };
};

interface IDetailProps extends WithStyles<typeof styles> {
  match: {
    params: {
      replicatorId: string;
    };
  };
}

const DetailView: React.FC<IDetailProps> = ({ classes, match }) => {
  const [replicator, setReplicator] = React.useState(Map());
  const [error, setError] = React.useState();
  const [status, setStatus] = React.useState();
  const [redirect, setRedirect] = React.useState(false);

  function getBaseParams() {
    return {
      namespace: getCurrentNamespace(),
      appName: match.params.replicatorId,
    };
  }

  React.useEffect(() => {
    MyReplicatorApi.getReplicator(getBaseParams()).subscribe((app) => {
      let config;

      try {
        config = JSON.parse(app.configuration);
      } catch (e) {
        setError(e);
      }

      setReplicator(fromJS(config));
    });

    MyReplicatorApi.pollStatus(getBaseParams()).subscribe((res) => {
      setStatus(res.status);
    });
  }, []);

  function start() {
    const params = {
      ...getBaseParams(),
      action: 'start',
    };

    MyReplicatorApi.action(params).subscribe(fetchStatus, (err) => {
      setError(err);
    });
  }

  function stop() {
    const params = {
      ...getBaseParams(),
      action: 'stop',
    };

    MyReplicatorApi.action(params).subscribe(fetchStatus, (err) => {
      setError(err);
    });
  }

  function deleteReplicator() {
    MyReplicatorApi.delete(getBaseParams()).subscribe(
      () => {
        setRedirect(true);
      },
      (err) => {
        setError(err);
      }
    );
  }

  function fetchStatus() {
    MyReplicatorApi.getStatus(getBaseParams()).subscribe((res) => {
      setStatus(res.status);
    });
  }

  const listViewLink = `/ns/${getCurrentNamespace()}/replicator`;

  if (redirect) {
    return <Redirect to={listViewLink} />;
  }

  const startTime = moment()
    .subtract(7, 'days')
    .format('X');
  let logUrl = `/v3/namespaces/${getCurrentNamespace()}/apps/${
    match.params.replicatorId
  }/workers/DeltaWorker/logs`;

  logUrl = `${logUrl}?start=${startTime}`;
  logUrl = `/downloadLogs?type=raw&backendPath=${encodeURIComponent(logUrl)}`;

  return (
    <div className={classes.root}>
      <div>
        <Link to={listViewLink}>Back to List View</Link>
      </div>
      <h2>{match.params.replicatorId}</h2>
      <br />

      <If condition={error}>
        <div className="text-danger">
          <strong>{JSON.stringify(error, null, 2)}</strong>
          <br />
        </div>
      </If>
      <div>
        <Status status={status} />
      </div>
      <br />
      <div className={classes.buttonContainer}>
        <Button variant="contained" color="primary" onClick={start} disabled={status === 'RUNNING'}>
          Start
        </Button>

        <Button variant="outlined" onClick={stop} disabled={status === 'STOPPED'}>
          Stop
        </Button>

        <Button variant="outlined" color="primary" href={logUrl} target="_tab">
          Logs
        </Button>

        <Button variant="contained" color="secondary" onClick={deleteReplicator}>
          DELETE
        </Button>
      </div>

      <br />
      <br />
      <div className={classes.config}>
        <pre>{JSON.stringify(replicator, null, 2)}</pre>
      </div>
    </div>
  );
};

const Detail = withStyles(styles)(DetailView);
export default Detail;
