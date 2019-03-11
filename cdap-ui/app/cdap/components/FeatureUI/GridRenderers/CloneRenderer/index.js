import React from 'react';

class CloneRenderer extends React.Component {
  constructor(props) {
    super(props);
  }
  invokeParentMethod(item) {
    if(this.props.context && this.props.context.componentParent && this.props.context.componentParent.onClone) {
      this.props.context.componentParent.onClone(item);
    }
  }
  render() {
    return <span className="fa fa-clone"
      onClick= {this.invokeParentMethod.bind(this, this.props.data)}>
    </span>

  }
}
export default CloneRenderer;
