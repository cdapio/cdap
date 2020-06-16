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
import ProgramDataFetcher from 'components/LogViewer/DataFetcher/ProgramDataFetcher';
import TextField from '@material-ui/core/TextField';
import Button from '@material-ui/core/Button';
import { getCurrentNamespace } from 'services/NamespaceStore';
import { MyProgramApi } from 'api/program';

const styles = (): StyleRules => {
  return {
    root: {
      padding: '10px 25px',
    },
    floatingButton: {
      transform: 'translateY(-50%)',
      position: 'absolute',
      right: 0,
      top: '50%',
    },
  };
};

const LogViewerPlayground: React.FC<WithStyles<typeof styles>> = ({ classes }) => {
  const [pipelineName, setPipelineName] = React.useState('');
  const [started, setStarted] = React.useState(false);
  const [dataFetcher, setDataFetcher] = React.useState<ProgramDataFetcher>(null);

  const [logs, setLogs] = React.useState([]);

  function startLog() {
    setStarted(true);

    const params = {
      namespace: getCurrentNamespace(),
      appId: pipelineName,
      programType: 'workflows',
      programId: 'DataPipelineWorkflow',
    };

    // get latest run
    MyProgramApi.runs(params).subscribe((res) => {
      const dataFetch = new ProgramDataFetcher({
        namespace: getCurrentNamespace(),
        application: pipelineName,
        programType: 'workflows',
        programName: 'DataPipelineWorkflow',
        runId: res[0].runid,
      });

      setDataFetcher(dataFetch);

      dataFetch.init().subscribe((logRes) => {
        setLogs(logRes);
      });
    });
  }

  function previous() {
    dataFetcher.getPrev().subscribe((res) => {
      const newLogs = res.concat(logs);
      setLogs(newLogs);
    });
  }

  function next() {
    dataFetcher.getNext().subscribe((res) => {
      const newLogs = logs.concat(res);
      setLogs(newLogs);
    });
  }

  function getLast() {
    dataFetcher.getLast().subscribe((res) => {
      setLogs(res);
    });
  }

  function getFirst() {
    dataFetcher.getFirst().subscribe((res) => {
      setLogs(res);
    });
  }

  return (
    <div className={classes.root}>
      <h1>Log Viewer</h1>

      <div className={classes.pipelineInfo}>
        <TextField
          label="Pipeline Name"
          value={pipelineName}
          onChange={(e) => setPipelineName(e.target.value)}
          variant="outlined"
          disabled={started}
        />
        <Button
          variant="contained"
          color="primary"
          onClick={startLog}
          disabled={started || pipelineName.length === 0}
        >
          Start Log Viewer
        </Button>
      </div>

      <br />
      <hr />
      <br />

      <div className={classes.logs}>
        <div>
          <Button variant="contained" color="primary" onClick={previous}>
            Previous
          </Button>
        </div>

        <div>
          <pre>
            {logs.map((logItem, i) => {
              return (
                <div key={`${logItem.offset}-${i}`}>
                  <span>{logItem.log.timestamp}</span>
                  <span>{` | `}</span>
                  <span>{logItem.log.message}</span>
                </div>
              );
            })}
          </pre>
        </div>

        <div>
          <Button variant="contained" color="primary" onClick={next}>
            Next
          </Button>
        </div>
      </div>

      <div className={classes.floatingButton}>
        <div>
          <Button variant="contained" color="primary" onClick={getFirst}>
            Beginning
          </Button>
        </div>
        <br />
        <div>
          <Button variant="contained" color="primary" onClick={getLast}>
            Last
          </Button>
        </div>
      </div>
    </div>
  );
};

const Playground = withStyles(styles)(LogViewerPlayground);
export default Playground;
