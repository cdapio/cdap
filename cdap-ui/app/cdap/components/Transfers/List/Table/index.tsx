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
import { transfersListConnect } from 'components/Transfers/List/context';
import withStyles, { WithStyles } from '@material-ui/core/styles/withStyles';
import TableRow from 'components/Transfers/List/Table/TableRow';
import T from 'i18n-react';

const PREFIX = 'features.Transfers.List.Table';

const styles = () => {
  return {
    table: {
      marginTop: '20px',
      '& .grid.grid-container .grid-row': {
        gridTemplateColumns: '2fr 200px 1fr 1fr 250px 50px',
      },
      '& .grid.grid-container': {
        maxHeight: 'calc(100vh - 350px)', // needs to be modified
      },
    },
  };
};

interface ITableProps extends WithStyles<typeof styles> {
  list: any[];
  getList: () => void;
}

const renderHeader = () => {
  return (
    <div className="grid-header header-bg-grey">
      <div className="grid-row">
        <div>{T.translate(`${PREFIX}.Headers.name`)}</div>
        <div>{T.translate(`${PREFIX}.Headers.stage`)}</div>
        <div>{T.translate(`${PREFIX}.Headers.from`)}</div>
        <div>{T.translate(`${PREFIX}.Headers.to`)}</div>
        <div>{T.translate(`${PREFIX}.Headers.lastUpdated`)}</div>
        <div />
      </div>
    </div>
  );
};

const renderBody = (list, getList) => {
  return (
    <div className="grid-body">
      {list.map((transfer) => {
        return <TableRow key={transfer.id} transfer={transfer} getList={getList} />;
      })}
    </div>
  );
};

const TableView: React.SFC<ITableProps> = ({ list, classes, getList }) => {
  return (
    <div className={`grid-wrapper ${classes.table}`}>
      <div className="grid grid-container">
        {renderHeader()}
        {renderBody(list, getList)}
      </div>
    </div>
  );
};

const StyledTable = withStyles(styles)(TableView);
const Table = transfersListConnect(StyledTable);
export default Table;
