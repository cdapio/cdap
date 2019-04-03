/*
 * Copyright © 2018 Cask Data, Inc.
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
import PipelineTable from 'components/PipelineList/DeployedPipelineView/PipelineTable';
import {
  fetchPipelineList,
  reset,
} from 'components/PipelineList/DeployedPipelineView/store/ActionCreator';
import PipelineCount from 'components/PipelineList/DeployedPipelineView/PipelineCount';
import SearchBox from 'components/PipelineList/DeployedPipelineView/SearchBox';
import Pagination from 'components/PipelineList/DeployedPipelineView/Pagination';
import { Provider } from 'react-redux';
import Store from 'components/PipelineList/DeployedPipelineView/store';

import './DeployedPipelineView.scss';

export default class DeployedPipelineView extends React.PureComponent {
  public componentDidMount() {
    fetchPipelineList();
  }

  public componentWillUnmount() {
    reset();
  }

  public render() {
    return (
      <Provider store={Store}>
        <div className="pipeline-deployed-view pipeline-list-content">
          <div className="deployed-header">
            <PipelineCount />
            <SearchBox />
            <Pagination />
          </div>

          <PipelineTable />
        </div>
      </Provider>
    );
  }
}
