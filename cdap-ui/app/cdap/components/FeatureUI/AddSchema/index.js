/* eslint react/prop-types: 0 */
import React from 'react';

require('./AddSchema.scss');

class AddSchema extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      title: props.title ? props.title : 'Add Schema',
      type: props.type ? props.type : 'NEW',
    };
  }

  onOperation(type) {
    if (this.props.operation) {
      this.props.operation(type, this.props.data);
    }
  }

  render() {
    return (
      <div className= { this.state.type == 'NEW' ? "add-container" : "scheme-container"}>
        <div className="tilte">{this.state.title}</div>
        {
          (this.state.type == 'NEW') ? <i className="fa fa-plus-circle add-operation" onClick={this.onOperation.bind(this,'ADD')}></i>:
          <div>
            <i className="fa fa-trash delete-operation" onClick={this.onOperation.bind(this,'REMOVE')}></i>
          </div>

        }
      </div>
    );
  }
}

export default AddSchema;
