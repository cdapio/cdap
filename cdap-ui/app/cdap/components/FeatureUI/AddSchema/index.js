import React from 'react';

require('./AddSchema.scss');

class AddSchema extends React.Component {
  constructor(props) {
    super(props);
    this.onClick = this.onClick.bind(this);
    this.state = {
      title: props.title ? props.title:'Add Schema',
      type: props.type ? props.type:'NEW',
    }
  }

  onClick() {
    if(this.props.operation){
      this.props.operation(this.state.type == 'NEW'? 'ADD': 'REMOVE', this.props.data);
    }
  }

  render() {
    return (
      <div className = "add-container">
        <div className = "tilte">{this.state.title}</div>
        <div className = "operation" onClick={this.onClick}>{this.state.type == 'NEW'? '+': 'x'}</div>
      </div>
    )
  }
}

export default AddSchema;