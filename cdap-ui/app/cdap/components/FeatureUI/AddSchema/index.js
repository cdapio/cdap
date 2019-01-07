/* eslint react/prop-types: 0 */
import React from 'react';

require('./AddSchema.scss');

class AddSchema extends React.Component {
  constructor(props) {
    super(props);
    this.onClick = this.onClick.bind(this);
    this.state = {
      title: props.title ? props.title : 'Schema',
      type: props.type ? props.type : 'NEW',
    };
  }

  onClick() {
    if (this.props.operation) {
      this.props.operation(this.state.type == 'NEW' ? 'ADD' : 'REMOVE', this.props.data);
    }
  }

  render() {
    return (
      <div className= { this.state.type == 'NEW' ? "add-container" : "scheme-container"}>
        <div className="tilte">{this.state.title}</div>
        {
          (this.state.type == 'NEW') ? <i class="fa fa-plus-circle add-operation" onClick={this.onClick}></i>:
          <div>
            <i class="fa fa-pencil edit-operation" onClick={this.onClick}></i>
            <i class="fa fa-trash delete-operation" onClick={this.onClick}></i>
          </div>

        }
      </div>
    );
  }
}

export default AddSchema;
