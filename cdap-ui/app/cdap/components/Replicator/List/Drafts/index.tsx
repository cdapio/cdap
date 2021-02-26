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
import { humanReadableDate, objectQuery } from 'services/helpers';
import { PluginType } from 'components/Replicator/constants';
import { Link } from 'react-router-dom';
import ActionsPopover, { IAction } from 'components/ActionsPopover';
import DeleteConfirmation, { InstanceType } from 'components/Replicator/DeleteConfirmation';
import DownloadFile from 'services/download-file';

const styles = (theme): StyleRules => {
  return {
    root: {
      height: '100%',
    },
    row: {
      color: theme.palette.grey[50],
      '&:hover': {
        color: 'inherit',
      },
    },
    headerText: {
      marginBottom: '10px',
    },
    gridWrapper: {
      // 100% - headerText
      height: 'calc(100% - 20px)',
      '& .grid.grid-container.grid-compact': {
        maxHeight: '100%',

        '& .grid-row': {
          gridTemplateColumns: '1fr 1fr 1fr 1fr 80px',
        },
      },
    },
    delete: {
      color: theme.palette.red[100],
    },
  };
};

const DraftsView: React.FC<WithStyles<typeof styles>> = ({ classes }) => {
  const [drafts, setDrafts] = React.useState([]);
  const [deleteReplicatorDraft, setDeleteReplicatorDraft] = React.useState(null);
  const [parentArtifact, setParentArtifact] = React.useState();

  function fetchDrafts() {
    const params = {
      namespace: getCurrentNamespace(),
    };

    MyReplicatorApi.getDeltaApp().subscribe((appInfo) => {
      setParentArtifact(appInfo.artifact);

      MyReplicatorApi.listDrafts(params).subscribe((list) => {
        setDrafts(list);
      });
    });
  }

  React.useEffect(fetchDrafts, []);

  function exportPipeline(replicationConfig) {
    const spec = {
      name: replicationConfig.label,
      artifact: parentArtifact,
      config: replicationConfig.config,
    };

    DownloadFile(spec);
  }

  return (
    <div className={classes.root}>
      <div className={classes.headerText}>
        {drafts.length} {drafts.length === 1 ? 'draft' : 'drafts'} - Select a row to edit draft
      </div>
      <div className={`grid-wrapper ${classes.gridWrapper}`}>
        <div className="grid grid-container grid-compact">
          <div className="grid-header">
            <div className="grid-row">
              <div>Draft name</div>
              <div>From / To</div>
              <div>Created</div>
              <div>Updated</div>
              <div />
            </div>
          </div>

          <div className="grid-body">
            {drafts.map((draft) => {
              const stageMap = {};
              const stages = objectQuery(draft, 'config', 'stages') || [];

              stages.forEach((stage) => {
                stageMap[stage.plugin.type] = stage.name;
              });

              const source = stageMap[PluginType.source] || '--';
              const target = stageMap[PluginType.target] || '--';

              const actions: IAction[] = [
                {
                  label: 'Export',
                  actionFn: () => exportPipeline(draft),
                },
                {
                  label: 'separator',
                },
                {
                  label: 'Delete',
                  actionFn: () => setDeleteReplicatorDraft(draft.name),
                  className: classes.delete,
                },
              ];

              return (
                <Link
                  to={`/ns/${getCurrentNamespace()}/replication/drafts/${draft.name}`}
                  className={`grid-row ${classes.row}`}
                  key={draft.name}
                >
                  <div>{draft.label}</div>
                  <div>
                    {source} / {target}
                  </div>
                  <div>{humanReadableDate(draft.createdTimeMillis, true)}</div>
                  <div>{humanReadableDate(draft.updatedTimeMillis, true)}</div>
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
        replicatorId={deleteReplicatorDraft}
        show={deleteReplicatorDraft && deleteReplicatorDraft.length > 0}
        onDelete={fetchDrafts}
        closeModal={() => setDeleteReplicatorDraft(null)}
        type={InstanceType.draft}
      />
    </div>
  );
};

const Drafts = withStyles(styles)(DraftsView);
export default Drafts;
