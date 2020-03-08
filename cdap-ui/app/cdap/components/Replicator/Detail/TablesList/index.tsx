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
import { detailContextConnect, IDetailContext } from 'components/Replicator/Detail';
import { generateTableKey } from 'components/Replicator/utilities';
import { Map } from 'immutable';
import { MyReplicatorApi } from 'api/replicator';
import { getCurrentNamespace } from 'services/NamespaceStore';

const styles = (theme): StyleRules => {
  return {
    root: {
      '& > .grid-wrapper': {
        height: '40vh',
        maxHeight: '40vh',

        '& .grid.grid-container.grid-compact': {
          maxHeight: '100%',

          '& .grid-row': {
            gridTemplateColumns: '150px 2fr 1fr',
          },
        },
      },
    },
  };
};

const TablesListView: React.FC<IDetailContext & WithStyles<typeof styles>> = ({
  classes,
  name,
  tables,
  columns,
  offsetBasePath,
}) => {
  const [statusMap, setStatusMap] = React.useState(Map());

  React.useEffect(() => {
    if (!offsetBasePath || offsetBasePath.length === 0) {
      return;
    }

    const params = {
      namespace: getCurrentNamespace(),
    };

    const body = {
      name,
      offsetBasePath,
    };

    // TODO: optimize polling
    // Don't poll when status is not running
    const statusPoll$ = MyReplicatorApi.pollTableStatus(params, body).subscribe((res) => {
      const tableStatus = res.tables;

      let status = Map();
      tableStatus.forEach((tableInfo) => {
        const tableKey = generateTableKey(tableInfo);
        status = status.set(tableKey, tableInfo.state);
      });

      setStatusMap(status);
    });

    return () => {
      statusPoll$.unsubscribe();
    };
  }, [offsetBasePath]);

  return (
    <div className={classes.root}>
      <div className="grid-wrapper">
        <div className="grid grid-container grid-compact">
          <div className="grid-header">
            <div className="grid-row">
              <div>Status</div>
              <div>Table name</div>
              <div>Selected columns</div>
            </div>
          </div>

          <div className="grid-body">
            {tables.keySeq().map((tableKey) => {
              const tableInfo = tables.get(tableKey);
              const numColumns = columns.get(tableKey).size;

              return (
                <div className="grid-row" key={tableKey.toString()}>
                  <div>{statusMap.get(tableKey) || '--'}</div>
                  <div>{tableInfo.get('table')}</div>
                  <div>{numColumns === 0 ? 'All' : numColumns}</div>
                </div>
              );
            })}
          </div>
        </div>
      </div>
    </div>
  );
};

const StyledTablesList = withStyles(styles)(TablesListView);
const TablesList = detailContextConnect(StyledTablesList);
export default TablesList;
