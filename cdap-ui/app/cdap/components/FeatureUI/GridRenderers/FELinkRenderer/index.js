import React from 'react';

class FSLinkRenderer extends React.Component {
  constructor(props) {
    super(props);
  }

  invokeParentMethod(item) {
    if (this.props.context && this.props.context.componentParent && this.props.context.componentParent.onEdit) {
      this.props.context.componentParent.onFeatureSelection(item);
    }
  }
  render() {
    return  <div className="view-link" onClick={this.invokeParentMethod.bind(this, this.props.data)}>
      {this.props.value}</div>
  }
}

export default FSLinkRenderer;
