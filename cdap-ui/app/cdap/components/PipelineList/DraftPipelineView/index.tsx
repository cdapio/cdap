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
import DraftTable from 'components/PipelineList/DraftPipelineView/DraftTable';
import { getDrafts, reset } from 'components/PipelineList/DraftPipelineView/store/ActionCreator';
import { Provider } from 'react-redux';
import Store from 'components/PipelineList/DraftPipelineView/store';
import DraftCount from 'components/PipelineList/DraftPipelineView/DraftCount';
import Pagination from 'components/PipelineList/DraftPipelineView/Pagination';

import './DraftPipelineView.scss';

export default class DraftPipelineView extends React.PureComponent {
  public componentDidMount() {
    getDrafts();
  }

  public componentWillUnmount() {
    reset();
  }

  public render() {
    return (
      <Provider store={Store}>
        <div className="pipeline-draft-view pipeline-list-content">
          <div className="draft-header">
            <DraftCount />
            <Pagination />
          </div>

          <DraftTable />
        </div>
      </Provider>
    );
  }
}
