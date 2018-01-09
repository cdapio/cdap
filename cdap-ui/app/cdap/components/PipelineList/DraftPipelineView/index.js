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

import React, { Component } from 'react';
import MyUserStoreApi from 'api/userstore';
import {getCurrentNamespace} from 'services/NamespaceStore';
import {objectQuery} from 'services/helpers';
import DraftTable from 'components/PipelineList/DraftPipelineView/DraftTable';
import T from 'i18n-react';

const PREFIX = 'features.PipelineList.DraftPipelineView';

require('./DraftPipelineView.scss');

export default class DraftPipelineView extends Component {
  componentWillMount() {
    MyUserStoreApi.get()
      .subscribe((res) => {
        let namespace = getCurrentNamespace();
        let draftsObj = objectQuery(res, 'property', 'hydratorDrafts', namespace) || {};

        let drafts = [];

        Object.keys(draftsObj)
          .forEach((id) => {
            drafts.push(draftsObj[id]);
          });

        this.setState({ drafts });
      });
  }

  state = {
    drafts: []
  };

  renderTable() {
    if (this.state.drafts.length === 0) { return null; }

    return <DraftTable drafts={this.state.drafts} />;
  }

  render() {
    return (
      <div className="pipeline-draft-view pipeline-list-content">
        <div className="draft-header">
          <div className="draft-count">
            {T.translate(`${PREFIX}.draftCount`, {count: this.state.drafts.length})}
          </div>
        </div>

        {this.renderTable()}
      </div>
    );
  }
}
