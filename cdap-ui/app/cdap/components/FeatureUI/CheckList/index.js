import React from 'react';
import isEmpty from 'lodash/isEmpty';
import cloneDeep from 'lodash/cloneDeep'
require('./CheckList.scss');

class CheckList extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      checkedItems: new Map(),
    }
  }

  componentWillReceiveProps(props) {
    if (!isEmpty(props.dataProvider)) {
      this.state.checkedItems = new Map();
      props.dataProvider.map((item, index) => {
        if (item.checked) {
          this.state.checkedItems.set(index, item.checked);
        }
      });
    }
  }

  onItemClick(index, event) {
    const isChecked = event.target.checked;
    this.setState(prevState => ({ checkedItems: prevState.checkedItems.set(index, isChecked) }));
    setTimeout(() =>  this.props.handleChange(this.state.checkedItems));
  }

  render() {
    let listData = this.props.dataProvider;
    let title = this.props.title;

    return (
      <div className="checklist-container">
        <div className="title">{title}</div>
        <div className='list'>
          {
            isEmpty(listData) ? 'No Data' : (
              listData.map((item, index) => {
                return <div className='list-item'>
                  <label className = 'check-box-container'>
                    <input type="checkbox" checked={this.state.checkedItems.get(index)} onClick={this.onItemClick.bind(this, index)} />
                    {item.name}
                  </label>
                  {
                    item.description && <div className='property'>{item.description}</div>
                  }
                </div>;
              })
            )}
        </div>
      </div>
    );
  }
}
export default CheckList;