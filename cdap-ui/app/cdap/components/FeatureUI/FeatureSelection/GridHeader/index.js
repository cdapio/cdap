import React, { Component } from 'react';

import './GridHeader.scss';

class GridHeader extends Component {

  constructor(props){
    super(props)
    this.state = {dummy:"hello"}
  }

  render() {

    return(
      <div className="grid-header-box">
      <div className="title-box">
      <h3>Feature Selection</h3>

      </div>
      <label className="subtitle">Pipeline: </label>
      {
        this.props.selectedPipeline &&
          <label>{this.props.selectedPipeline.pipelineName}</label>
      }
      </div>
    )
  }
}

export default GridHeader;
