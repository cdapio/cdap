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
import SourceList from 'components/Replicator/List/SourceList';
import { MyReplicatorApi } from 'api/replicator';
import { getCurrentNamespace } from 'services/NamespaceStore';
import moment from 'moment';

const Status = ({ appName }) => {
  const [programStatus, setProgramStatus] = React.useState();

  React.useEffect(() => {
    const params = {
      namespace: getCurrentNamespace(),
      appName,
    };

    const status$ = MyReplicatorApi.pollStatus(params).subscribe((res) => {
      setProgramStatus(res.status);
    });

    return () => {
      status$.unsubscribe();
    };
  });

  return <span className="ml-3">{programStatus}</span>;
};

export default class List extends React.PureComponent {
  public state = {
    replicators: [],
  };

  public componentDidMount() {
    this.fetchList();
  }

  private fetchList = () => {
    MyReplicatorApi.list({ namespace: getCurrentNamespace() }).subscribe((res) => {
      this.setState({ replicators: res });
    });
  };

  private start = (appName) => {
    const params = {
      namespace: getCurrentNamespace(),
      appName,
      action: 'start',
    };

    MyReplicatorApi.action(params).subscribe(
      () => {
        // tslint:disable-next-line: no-console
        console.log(`${appName} started`);
      },
      (err) => {
        // tslint:disable-next-line: no-console
        console.log(err);
      }
    );
  };

  private stop = (appName) => {
    const params = {
      namespace: getCurrentNamespace(),
      appName,
      action: 'stop',
    };

    MyReplicatorApi.action(params).subscribe(
      () => {
        // tslint:disable-next-line: no-console
        console.log(`${appName} stopped`);
      },
      (err) => {
        // tslint:disable-next-line: no-console
        console.log(err);
      }
    );
  };

  private delete = (appName) => {
    const params = {
      namespace: getCurrentNamespace(),
      appName,
    };

    MyReplicatorApi.delete(params).subscribe(this.fetchList, (err) => {
      // tslint:disable-next-line: no-console
      console.log(err);
    });
  };

  public render() {
    return (
      <div className="container">
        <h1>Replicator List</h1>
        <br />
        <SourceList />

        <h2>Replicators</h2>
        <ul>
          {this.state.replicators.map((replicator) => {
            const startTime = moment()
              .subtract(7, 'days')
              .format('X');
            let logUrl = `/v3/namespaces/${getCurrentNamespace()}/apps/${
              replicator.name
            }/workers/DeltaWorker/logs`;

            logUrl = `${logUrl}?start=${startTime}`;
            logUrl = `/downloadLogs?type=raw&backendPath=${encodeURIComponent(logUrl)}`;

            return (
              <li key={replicator.name}>
                <strong>{replicator.name}</strong>
                <Status appName={replicator.name} />
                <span className="btn btn-link" onClick={this.start.bind(this, replicator.name)}>
                  Start
                </span>
                <span className="btn btn-link" onClick={this.stop.bind(this, replicator.name)}>
                  Stop
                </span>
                <span>
                  <a href={logUrl} target="_tab">
                    Logs
                  </a>
                </span>
                <span
                  className="btn btn-danger ml-3"
                  onClick={this.delete.bind(this, replicator.name)}
                >
                  Delete
                </span>
              </li>
            );
          })}
        </ul>
      </div>
    );
  }
}
