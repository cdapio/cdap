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
import PropTypes from 'prop-types';
import classnames from 'classnames';
import IconSVG from 'components/IconSVG';
import PipelineSummary from 'components/PipelineSummary';
import {getCurrentNamespace} from 'services/NamespaceStore';
import PipelineDetailStore from 'components/PipelineDetails/store';
import {GLOBALS} from 'services/global-constants';
import T from 'i18n-react';

const PREFIX = 'features.PipelineDetails.TopPanel';

export default class PipelineSummaryButton extends Component {
  static propTypes = {
    isBatch: PropTypes.bool,
    pipelineName: PropTypes.string,
    setActiveButton: PropTypes.func
  };

  state = {
    showSummary: false,
    mouseIsOver: false
  };

  toggleSummary = () => {
    this.setState({
      showSummary: !this.state.showSummary
    }, this.setActiveButton);
  };

  setMouseOver = (value) => {
    this.setState({
      mouseIsOver: value
    }, this.setActiveButton);
  };

  setActiveButton = () => {
    if (this.state.showSummary || this.state.mouseIsOver) {
      this.props.setActiveButton(true);
    } else {
      this.props.setActiveButton(false);
    }
  };

  renderSummaryButton() {
    return (
      <div
        onClick={this.toggleSummary}
        className={classnames("btn pipeline-action-btn pipeline-summary-btn", {"btn-select" : this.state.showSummary})}
      >
        <div className="btn-container">
          <IconSVG
            name="icon-line-chart"
            className="summary-icon"
          />
          <div className="button-label">
            {T.translate(`${PREFIX}.summary`)}
          </div>
        </div>
      </div>
    );
  }

  render() {
    let pipelineType = this.props.isBatch ? GLOBALS.etlDataPipeline : GLOBALS.etlDataStreams;
    let programType = GLOBALS.programType[pipelineType];
    let programId = GLOBALS.programId[pipelineType];

    return (
      <div
        className={classnames("pipeline-action-container pipeline-summary-container", {"active" : this.state.showSummary})}
        onMouseEnter={this.setMouseOver.bind(this, true)}
        onMouseLeave={this.setMouseOver.bind(this, false)}
      >
        {this.renderSummaryButton()}
        {
          this.state.showSummary ?
            <PipelineSummary
              pipelineType={pipelineType}
              namespaceId={getCurrentNamespace()}
              appId={this.props.pipelineName}
              programType={programType}
              programId={programId}
              pipelineConfig={PipelineDetailStore.getState()}
              totalRunsCount={PipelineDetailStore.getState().runs.length}
              onClose={this.toggleSummary}
            />
          :
            null
        }
      </div>
    );
  }
}
