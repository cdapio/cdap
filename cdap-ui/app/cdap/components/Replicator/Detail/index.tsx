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
import { getCurrentNamespace } from 'services/NamespaceStore';
import { MyReplicatorApi } from 'api/replicator';
import { List, Map, fromJS } from 'immutable';
import { Redirect } from 'react-router-dom';
import TopPanel from 'components/Replicator/Detail/TopPanel';
import { objectQuery } from 'services/helpers';
import { PROGRAM_STATUSES } from 'services/global-constants';
import { Observable } from 'rxjs/Observable';
import { PluginType } from 'components/Replicator/constants';
import {
  fetchPluginInfo,
  fetchPluginWidget,
  generateTableKey,
} from 'components/Replicator/utilities';
import ConfigDisplay from 'components/Replicator/ConfigDisplay';
import TablesList from 'components/Replicator/Detail/TablesList';
import Metrics from 'components/Replicator/Detail/Metrics';
import ThroughputLatencyGraphs from 'components/Replicator/Detail/ThroughputLatencyGraphs';

export const DetailContext = React.createContext<Partial<IDetailState>>({});

const styles = (theme): StyleRules => {
  return {
    buttonContainer: {
      '& > *': {
        marginRight: '15px',
      },
    },
    config: {
      border: `1px solid ${theme.palette.grey[300]}`,
      borderRadius: '4px',
      wordBreak: 'break-word',
      whiteSpace: 'pre-wrap',
      padding: '15px',
    },
    body: {
      padding: '15px 40px',
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

interface IColumn {
  name: string;
  type: string;
}

interface IDetailState {
  name: string;
  description: string;
  status: string;
  redirect: boolean;
  rawAppConfig: Map<string, any>;
  runId: string;
  sourcePluginInfo: any;
  sourcePluginWidget: any;
  targetPluginInfo: any;
  targetPluginWidget: any;
  sourceConfig: Record<string, string>;
  targetConfig: Record<string, string>;
  tables: Map<string, Map<string, string>>;
  columns: Map<string, List<IColumn>>;
  offsetBasePath: string;

  start: () => void;
  stop: () => void;
  deleteReplicator: () => void;
}

export type IDetailContext = Partial<IDetailState>;

class DetailView extends React.PureComponent<IDetailProps, IDetailContext> {
  private statusPoll$ = null;

  private start = () => {
    const params = {
      ...this.getBaseParams(),
      action: 'start',
    };

    MyReplicatorApi.action(params).subscribe(
      () => {
        this.setState({
          status: PROGRAM_STATUSES.STARTING,
        });
      },
      (err) => {
        // tslint:disable-next-line: no-console
        console.log('error', err);
      }
    );
  };

  private stop = () => {
    const currentStatus = this.state.status;

    this.setState({
      status: PROGRAM_STATUSES.STOPPING,
    });

    const params = {
      ...this.getBaseParams(),
      action: 'stop',
    };

    MyReplicatorApi.action(params).subscribe(
      () => {
        this.setState({
          status: PROGRAM_STATUSES.STOPPING,
        });
      },
      (err) => {
        // tslint:disable-next-line: no-console
        console.log('error', err);
        this.setState({
          status: currentStatus,
        });
      }
    );
  };

  private deleteReplicator = () => {
    MyReplicatorApi.delete(this.getBaseParams()).subscribe(
      () => {
        this.setState({ redirect: true });
      },
      (err) => {
        // tslint:disable-next-line: no-console
        console.log('error', err);
      }
    );
  };

  public state = {
    name: objectQuery(this.props, 'match', 'params', 'replicatorId'),
    description: null,
    status: null,
    runId: null,
    redirect: false,
    rawAppConfig: null,
    sourcePluginInfo: null,
    sourcePluginWidget: null,
    targetPluginInfo: null,
    targetPluginWidget: null,
    sourceConfig: {},
    targetConfig: {},
    tables: Map<string, Map<string, string>>(),
    columns: Map<string, List<IColumn>>(),
    offsetBasePath: '',

    start: this.start,
    stop: this.stop,
    deleteReplicator: this.deleteReplicator,
  };

  public componentDidMount() {
    this.init();
  }

  public componentWillUnmount() {
    if (this.statusPoll$) {
      this.statusPoll$.unsubscribe();
    }
  }

  // TODO: refactor to unify with Draft init
  private init = () => {
    MyReplicatorApi.getReplicator(this.getBaseParams()).subscribe((app) => {
      const parentArtifact = { ...app.artifact };

      let config;
      try {
        config = JSON.parse(app.configuration);
      } catch (e) {
        // tslint:disable-next-line: no-console
        console.log('error parsing app config', e);
      }

      let sourcePlugin$;
      let targetPlugin$;
      let sourceWidget$;
      let targetWidget$;
      let sourceConfig;
      let targetConfig;

      config.stages.forEach((stage) => {
        const artifactName = stage.plugin.artifact.name;
        const artifactVersion = stage.plugin.artifact.version;
        const artifactScope = stage.plugin.artifact.scope;
        const pluginName = stage.plugin.name;
        const pluginType = stage.plugin.type;
        const pluginConfig = stage.plugin.properties;

        if (pluginType === PluginType.source) {
          sourcePlugin$ = fetchPluginInfo(
            parentArtifact,
            artifactName,
            artifactScope,
            pluginName,
            pluginType
          );
          sourceWidget$ = fetchPluginWidget(
            artifactName,
            artifactVersion,
            artifactScope,
            pluginName,
            pluginType
          );
          sourceConfig = pluginConfig;
        } else {
          targetPlugin$ = fetchPluginInfo(
            parentArtifact,
            artifactName,
            artifactScope,
            pluginName,
            pluginType
          );
          targetWidget$ = fetchPluginWidget(
            artifactName,
            artifactVersion,
            artifactScope,
            pluginName,
            pluginType
          );
          targetConfig = pluginConfig;
        }
      });

      // fetch plugins
      Observable.combineLatest(
        sourcePlugin$,
        sourceWidget$,
        targetPlugin$,
        targetWidget$
      ).subscribe(
        ([sourcePluginInfo, sourcePluginWidget, targetPluginInfo, targetPluginWidget]) => {
          this.setState({
            sourcePluginInfo,
            sourcePluginWidget,
            sourceConfig,
            targetPluginInfo,
            targetPluginWidget,
            targetConfig,
          });
        },
        (err) => {
          // tslint:disable-next-line: no-console
          console.log('error fetching plugins', err);
        }
      );

      let selectedTables = Map<string, Map<string, string>>();
      const tables = objectQuery(config, 'tables') || [];
      let columns = Map<string, List<IColumn>>();

      tables.forEach((table) => {
        const tableKey = generateTableKey(table);

        selectedTables = selectedTables.set(
          tableKey,
          fromJS({
            database: table.database,
            table: table.table,
          })
        );

        const tableColumns = objectQuery(table, 'columns') || [];
        const columnList = fromJS(tableColumns);

        columns = columns.set(tableKey, columnList);
      });

      this.setState({
        rawAppConfig: fromJS(config),
        name: objectQuery(this.props, 'match', 'params', 'replicatorId'),
        description: app.description,
        tables: selectedTables,
        columns,
        offsetBasePath: config.offsetBasePath,
      });
    });

    this.getStatus();
  };

  private getStatus = () => {
    this.statusPoll$ = MyReplicatorApi.pollStatus(this.getBaseParams()).subscribe((runsInfo) => {
      if (runsInfo.length === 0) {
        this.setState({
          status: PROGRAM_STATUSES.DEPLOYED,
        });
        return;
      }

      const latestRun = runsInfo[0];

      this.setState({
        status: latestRun.status,
        runId: latestRun.runid,
      });
    });
  };

  private getBaseParams = () => {
    return {
      namespace: getCurrentNamespace(),
      appName: this.props.match.params.replicatorId,
    };
  };

  private redirect = () => {
    const listViewLink = `/ns/${getCurrentNamespace()}/replication`;
    return <Redirect to={listViewLink} />;
  };

  public render() {
    if (this.state.redirect) {
      return this.redirect();
    }

    const classes = this.props.classes;

    return (
      <DetailContext.Provider value={this.state}>
        <div>
          <TopPanel />

          <div className={classes.body}>
            <ConfigDisplay
              sourcePluginInfo={this.state.sourcePluginInfo}
              targetPluginInfo={this.state.targetPluginInfo}
              sourcePluginWidget={this.state.sourcePluginWidget}
              targetPluginWidget={this.state.targetPluginWidget}
              sourceConfig={this.state.sourceConfig}
              targetConfig={this.state.targetConfig}
            />

            <hr />
            <ThroughputLatencyGraphs />
            <TablesList />
          </div>
        </div>
      </DetailContext.Provider>
    );
  }
}

export function detailContextConnect(Comp) {
  return (extraProps) => {
    return (
      <DetailContext.Consumer>
        {(props) => {
          const finalProps = {
            ...props,
            ...extraProps,
          };

          return <Comp {...finalProps} />;
        }}
      </DetailContext.Consumer>
    );
  };
}

const Detail = withStyles(styles)(DetailView);
export default Detail;
