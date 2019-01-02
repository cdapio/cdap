import React from 'react';
import isEmpty from 'lodash/isEmpty';
import { Input } from 'reactstrap';

require('./NameValueList.scss');

class NameValueList extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      newName: '',
      newValue: ''
    }
  }
  onNewNameChange(event) {
    this.setState({
      newName: event.target.value
    })
  }

  onNewValueChange(event) {
    this.setState({
      newValue: event.target.value
    })
  }

  onValueUpdated(index, item, event) {
    this.props.updateNameValue(index, {...item,
      value: event.target.value});
  }

  onAdd() {
    if(!isEmpty(this.state.newName) && !isEmpty(this.state.newValue)){
      this.props.addNameValue({
        name : this.state.newName,
        value: this.state.newValue,
        dataType: "string",
        isCollection: false
      });
      this.setState({
        newName: '',
        newValue: ''
      });
    }
  }

  render() {
    let listData = isEmpty(this.props.dataProvider) ? [] : this.props.dataProvider;
    console.log("Rendering list ", listData);
    return (
      <div>
        {
          listData.map((item, index) => {
            return (
              <div className='list-row'>
                <div className='name'>{item.name}</div>
                <div className='colon'>:</div>
                <Input className='value' type="text" name="value" placeholder='value'
                  defaultValue={item.value} onChange={this.onValueUpdated.bind(this,index,item)}/>
                {
                  item.toolTip &&
                  <i className="fa fa-info-circle field-info" title = {item.toolTip}></i>
                }
              </div>
            );
          })
        }
        <div className='list-row'>
          <Input className='value' type="text" name="value" placeholder='Custom Key'
            value = {this.state.newName} onChange={this.onNewNameChange.bind(this)} />
          <div className='colon'>:</div>
          <Input className='value' type="text" name="value" placeholder='value'
            value = {this.state.newValue} onChange={this.onNewValueChange.bind(this)} />
        </div>
        <button onClick = {this.onAdd.bind(this)}  >Add</button>
      </div>
    );
  }
}
export default NameValueList;