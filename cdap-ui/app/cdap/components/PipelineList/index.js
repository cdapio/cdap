/*
 * Copyright © 2017 Cask Data, Inc.
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
import classnames from 'classnames';
import DeployedPipelineView from 'components/PipelineList/DeployedPipelineView';
import ResourceCenterButton from 'components/ResourceCenterButton';
import DraftPipelineView from 'components/PipelineList/DraftPipelineView';
import T from 'i18n-react';

require('./PipelineList.scss');

const VIEWS = {
  deployed: 'DEPLOYED',
  draft: 'DRAFT'
};

const PREFIX = 'features.PipelineList';

export default class PipelineList extends Component {
  constructor(props) {
    super(props);
  }

  state = {
    activeView: VIEWS.deployed
  };

  changeView(view) {
    if (this.state.activeView === view) { return; }

    this.setState({activeView: view});
  }

  renderContent() {
    if (this.state.activeView === VIEWS.deployed) {
      return <DeployedPipelineView />;
    } else {
      return <DraftPipelineView />;
    }
  }

  render() {
    return (
      <div className="pipeline-list">
        <div className="view-header">
          <span
            className={classnames('option', {'active': this.state.activeView === VIEWS.deployed})}
            onClick={this.changeView.bind(this, VIEWS.deployed)}
          >
            {T.translate(`${PREFIX}.deployed`)}
          </span>
          <span className="separator">|</span>
          <span
            className={classnames('option', {'active': this.state.activeView === VIEWS.draft})}
            onClick={this.changeView.bind(this, VIEWS.draft)}
          >
            {T.translate(`${PREFIX}.draft`)}
          </span>
        </div>

        <ResourceCenterButton />

        {this.renderContent()}
      </div>
    );
  }
}
