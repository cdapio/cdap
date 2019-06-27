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
import ActionsPopover from 'components/ActionsPopover';
import { start, stop, deleteApp } from 'components/Transfers/utilities';
import T from 'i18n-react';

const PREFIX = 'features.Transfers.Actions';

interface ITableRowProps {
  transfer: any;
  getList: () => void;
  status: string;
}

const TableRow: React.SFC<ITableRowProps> = ({ transfer, getList, status }) => {
  const actions = [
    {
      label: T.translate(`${PREFIX}.start`),
      actionFn: start.bind(null, transfer, getList),
    },
    {
      label: T.translate(`${PREFIX}.stop`),
      actionFn: stop.bind(null, transfer, getList),
    },
    {
      label: 'separator',
    },
    {
      label: T.translate(`${PREFIX}.logs`),
    },
    {
      label: 'separator',
    },
    {
      label: T.translate(`${PREFIX}.delete`),
      className: 'delete',
      actionFn: deleteApp.bind(null, transfer, getList),
    },
  ];

  return (
    <div className="grid-row" key={transfer.name}>
      <div>{transfer.name}</div>
      <div>{status}</div>
      <div>Oracle</div>
      <div>BigQuery</div>
      <div>
        <ActionsPopover actions={actions} />
      </div>
    </div>
  );
};

export default TableRow;
