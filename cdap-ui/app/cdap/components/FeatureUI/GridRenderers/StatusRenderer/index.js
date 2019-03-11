import React from 'react';
import { SUCCEEDED, FAILED, DEPLOYED, RUNNING } from 'components/FeatureUI/config';
require('./StatusRenderer.scss');

class StatusRenderer extends React.Component {
  constructor(props) {
    super(props);
  }
  refresh() {
    return true;
  }
  getValue = () => {
    return this.props.value;
  };
  render() {
    return <div>
      <span className = {this.getStatusClass(this.props.value)}></span>
      {this.props.value}
    </div>;
  }
  getStatusClass(status) {
    let className = "fa fa-circle status-padding";
    switch (status) {
      case SUCCEEDED:
        className += " status-success";
        break;
      case FAILED:
        className += " status-failed";
        break;
      case DEPLOYED:
        className += " status-deployed";
        break;
      case RUNNING:
        className += " status-running";
        break;
    }
    return className;
  }
}
export default StatusRenderer;
