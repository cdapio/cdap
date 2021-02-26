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
import { MyReplicatorApi } from 'api/replicator';
import { getCurrentNamespace } from 'services/NamespaceStore';
import { PluginType } from 'components/Replicator/constants';
import { objectQuery } from 'services/helpers';
import Status from 'components/Status';
import { Link } from 'react-router-dom';
import ActionsPopover, { IAction } from 'components/ActionsPopover';
import DeleteConfirmation, { InstanceType } from 'components/Replicator/DeleteConfirmation';
import DownloadFile from 'services/download-file';
import { Redirect } from 'react-router-dom';

const styles = (theme): StyleRules => {
  return {
    root: {
      height: '100%',
    },
    headerText: {
      marginBottom: '10px',
    },
    row: {
      color: theme.palette.grey[50],
      '&:hover': {
        color: 'inherit',
      },
    },
    gridWrapper: {
      // 100% - headerText
      height: 'calc(100% - 20px)',
      '& .grid.grid-container.grid-compact': {
        maxHeight: '100%',
        '& .grid-row': {
          gridTemplateColumns: '2fr 1fr 1fr 80px',
        },
      },
    },
    delete: {
      color: theme.palette.red[100],
    },
  };
};

const DeployedView: React.FC<WithStyles<typeof styles>> = ({ classes }) => {
  const [replicators, setReplicators] = React.useState([]);
  const [statusMap, setStatusMap] = React.useState({});
  const [configMap, setConfigMap] = React.useState({});
  const [replicatorNameDelete, setReplicatorNameDelete] = React.useState(null);
  const [redirect, setRedirect] = React.useState<string>();

  // TODO: Replace with GraphQL
  function fetchList() {
    const params = {
      namespace: getCurrentNamespace(),
    };

    MyReplicatorApi.list(params).subscribe((list) => {
      setReplicators(list);

      // Batch status
      const batchStatusBody = list.map((replicator) => {
        return {
          appId: replicator.name,
          programType: 'worker',
          programId: 'DeltaWorker',
        };
      });

      MyReplicatorApi.batchStatus(params, batchStatusBody).subscribe((status) => {
        const map = {};

        status.forEach((replicator) => {
          map[replicator.appId] = replicator.status;
        });

        setStatusMap(map);
      });

      const batchDetailBody = list.map((replicator) => {
        return {
          appId: replicator.name,
        };
      });

      MyReplicatorApi.batchAppDetail(params, batchDetailBody).subscribe((apps) => {
        const map = {};

        apps.forEach((app) => {
          if (!app.detail) {
            return;
          }
          const replicator = app.detail;

          let config;
          try {
            config = JSON.parse(replicator.configuration);
          } catch (e) {
            // tslint:disable-next-line: no-console
            console.log('Failed to parse replication pipeline configuration', e);
            return;
          }

          const connection = objectQuery(config, 'connections', 0);
          const replicatorObj = {
            from: connection.from,
            to: connection.to,
          };

          map[replicator.name] = {
            name: replicator.name,
            artifact: replicator.artifact,
            config,
            pluginDisplay: replicatorObj,
          };
        });

        setConfigMap(map);
      });
    });
  }

  React.useEffect(fetchList, []);

  function getPipelineConfig(replicationName) {
    const replicationObj = objectQuery(configMap, replicationName);

    const replicationConfig = {
      name: replicationObj.name,
      artifact: replicationObj.artifact,
      config: replicationObj.config,
    };

    return replicationConfig;
  }

  function exportPipeline(replicationName) {
    const replicationConfig = getPipelineConfig(replicationName);
    DownloadFile(replicationConfig);
  }

  function duplicatePipeline(replicationName) {
    const replicationObj = getPipelineConfig(replicationName);
    const replicationConfig = {
      ...replicationObj,
      name: '',
    };

    const cloneId = replicationObj.name;
    window.localStorage.setItem(cloneId, JSON.stringify(replicationConfig));

    const createViewLink = `/ns/${getCurrentNamespace()}/replication/create?cloneId=${cloneId}`;
    setRedirect(createViewLink);
  }

  if (redirect) {
    return <Redirect to={redirect} />;
  }

  return (
    <div className={classes.root}>
      <div className={classes.headerText}>{replicators.length} deployed</div>

      <div className={`grid-wrapper ${classes.gridWrapper}`}>
        <div className="grid grid-container grid-compact">
          <div className="grid-header">
            <div className="grid-row">
              <div>Name</div>
              <div>From / To</div>
              <div>Status</div>
              <div />
            </div>
          </div>

          <div className="grid-body">
            {replicators.map((replicator) => {
              const source =
                objectQuery(configMap, replicator.name, 'pluginDisplay', 'from') || '--';
              const target = objectQuery(configMap, replicator.name, 'pluginDisplay', 'to') || '--';

              const actions: IAction[] = [
                {
                  label: 'Export',
                  actionFn: () => exportPipeline(replicator.name),
                },
                {
                  label: 'Duplicate',
                  actionFn: () => duplicatePipeline(replicator.name),
                },
                {
                  label: 'separator',
                },
                {
                  label: 'Delete',
                  actionFn: () => setReplicatorNameDelete(replicator.name),
                  className: classes.delete,
                },
              ];

              return (
                <Link
                  to={`/ns/${getCurrentNamespace()}/replication/detail/${replicator.name}`}
                  className={`grid-row ${classes.row}`}
                  key={replicator.name}
                >
                  <div>{replicator.name}</div>
                  <div>
                    {source} / {target}
                  </div>
                  <div>
                    <Status status={statusMap[replicator.name]} />
                  </div>
                  <div>
                    <ActionsPopover actions={actions} />
                  </div>
                </Link>
              );
            })}
          </div>
        </div>
      </div>

      <DeleteConfirmation
        replicatorId={replicatorNameDelete}
        show={replicatorNameDelete && replicatorNameDelete.length > 0}
        onDelete={fetchList}
        closeModal={() => setReplicatorNameDelete(null)}
        type={InstanceType.app}
      />
    </div>
  );
};

const Deployed = withStyles(styles)(DeployedView);
export default Deployed;
