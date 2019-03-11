import React from 'react';

class EditRenderer extends React.Component {
  constructor(props) {
    super(props);
  }

  invokeParentMethod(item) {
    if(this.props.context && this.props.context.componentParent && this.props.context.componentParent.onEdit) {
      this.props.context.componentParent.onEdit(item);
    }
  }
  render() {
    return <span className="fa fa-edit"
      onClick= {this.invokeParentMethod.bind(this, this.props.data)}>
    </span>

  }
}
export default EditRenderer;
