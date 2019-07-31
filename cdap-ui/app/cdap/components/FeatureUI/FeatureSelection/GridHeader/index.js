
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
import PropTypes from 'prop-types';
import './GridHeader.scss';

class GridHeader extends Component {

  constructor(props) {
    super(props);
    this.state = { dummy: "hello" };
  }

  render() {

    return (
      <div className="grid-header-box">
        <div className="title-box">
          <h3>Feature Selection</h3>
          <div className="subtitle">
            <label className="subtitle-label">Pipeline: </label>
            {
              this.props.selectedPipeline &&
              <label className="pipeline-name">{this.props.selectedPipeline.pipelineName}</label>
            }
          </div>
        </div>
        <div className="header-control">
          <div className = "selected-feature">{ "Selected Features: " + this.props.selectedCount }</div>
          <button className = "feature-button left-margin" onClick={this.props.backnavigation}>Back</button>
          <button className = "feature-button left-margin" onClick={this.props.save}
            disabled={!this.props.enableSave}>Save</button>
        </div>
      </div>
    );
  }
}

export default GridHeader;
GridHeader.propTypes = {
  selectedPipeline: PropTypes.object,
  backnavigation: PropTypes.func,
  save: PropTypes.func,
  enableSave: PropTypes.any,
  selectedCount: PropTypes.number,
  totalCount: PropTypes.number
};
