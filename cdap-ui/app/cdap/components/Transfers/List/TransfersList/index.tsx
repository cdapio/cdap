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
import { MyDeltaApi } from 'api/delta';
import { getCurrentNamespace } from 'services/NamespaceStore';
import { TransfersListContext, defaultContext } from 'components/Transfers/List/context';
import Count from 'components/Transfers/List/Count';
import Table from 'components/Transfers/List/Table';
import { getStatuses } from 'components/Transfers/utilities';

const parentArtifact = 'delta-app';

interface IState {
  list: any[];
  statuses: any;
  getList: () => void;
}

export default class TransfersList extends React.PureComponent<{}, IState> {
  public getTransfersList = () => {
    MyDeltaApi.list({ context: getCurrentNamespace() }).subscribe((res) => {
      this.setState({
        list: res,
      });
    });
  };

  public state = {
    ...defaultContext,
    getList: this.getTransfersList,
  };

  public componentDidMount() {
    this.getTransfersList();
  }

  // private getStatuses = () => {
  //   getStatuses(this.state.list).subscribe((res) => {
  //     const statuses = {};

  //     res.forEach((app) => {
  //       statuses[app.appId] = app.status;
  //     });

  //     this.setState({ statuses });
  //   });
  // };

  public render() {
    return (
      <div>
        <TransfersListContext.Provider value={this.state}>
          <Count />
          <Table />
        </TransfersListContext.Provider>
      </div>
    );
  }
}
