import React from 'react';

class DeleteRenderer extends React.Component {
  constructor(props) {
    super(props);
  }
  invokeParentMethod(item) {
    if(this.props.context && this.props.context.componentParent && this.props.context.componentParent.onDelete) {
      this.props.context.componentParent.onDelete(item);
    }
  }
  render() {
    return <span className="fa fa-trash text-danger"
      onClick= {this.invokeParentMethod.bind(this, this.props.data)}>
    </span>

  }
}
export default DeleteRenderer;
