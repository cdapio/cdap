/* eslint react/prop-types: 0 */
import React, { Component } from 'react';

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
          <button className = "feature-button left-margin" onClick={this.props.backnavigation}>Back</button>
          <button className = "feature-button left-margin" onClick={this.navigateToParentWindow}>Save</button>
        </div>
      </div>
    );
  }
}

export default GridHeader;
