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
import PipelineDetailsMetadata from 'components/PipelineDetails/PipelineDetailsTopPanel/PipelineDetailsMetadata';
import PipelineDetailsButtons from 'components/PipelineDetails/PipelineDetailsTopPanel/PipelineDetailsButtons';
import PipelineDetailStore from 'components/PipelineDetails/store';
import {fetchMacros} from 'components/PipelineDetails/store/ActionCreator';
import {getCurrentNamespace} from 'services/NamespaceStore';

require('./PipelineDetailsTopPanel.scss');

export default class PipelineDetailsTopPanel extends Component {
  componentDidMount() {
    fetchMacros({
      namespace: getCurrentNamespace(),
      appId: PipelineDetailStore.getState().name
    });
  }
  render() {
    return (
      <div className = "pipeline-details-top-panel">
        <PipelineDetailsMetadata />
        <PipelineDetailsButtons />
      </div>
    );
  }
}
