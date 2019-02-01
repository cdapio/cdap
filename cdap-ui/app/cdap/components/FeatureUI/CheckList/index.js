/* eslint react/prop-types: 0 */
import React from 'react';
import isEmpty from 'lodash/isEmpty';
require('./CheckList.scss');

class CheckList extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      checkedItems: this.getCheckedItemsFromProps(props)
    };
  }

  componentWillReceiveProps(props) {
    this.setState({
      checkedItems: this.getCheckedItemsFromProps(props)
    });
  }

  getCheckedItemsFromProps(props) {
    let checkedItems = new Map();
    if (!isEmpty(props.dataProvider)) {
      props.dataProvider.map((item, index) => {
        if (item.checked) {
          checkedItems.set(index, item.checked);
        }
      });
    }
    return checkedItems;
  }

  onItemClick(index, event) {
    const isChecked = event.target.checked;
    if (this.props.isSingleSelect) {
      let checkedItem = new Map();
      checkedItem.set(index, isChecked);
      this.setState({ checkedItems: checkedItem });
    } else {
      this.setState(prevState => ({ checkedItems: prevState.checkedItems.set(index, isChecked) }));
    }
    setTimeout(() => this.props.handleChange(this.state.checkedItems));
  }

  render() {
    let listData = this.props.dataProvider;
    let title = this.props.title;

    return (
      <div className="checklist-container">
        {
          title && <div className="title">{title}</div>
        }
        <div className='list'>
          {
            isEmpty(listData) ? 'No Data' : (
              listData.map((item, index) => {
                return (
                  <div className='list-item' key={item.name}>
                    <label className='check-box-container'>
                      <input type="checkbox"
                      checked = {this.state.checkedItems.get(index) || false}
                      onChange={this.onItemClick.bind(this, index)} />
                      {item.name}
                    </label>
                    {
                      item.description && <div className='property'>{item.description}</div>
                    }
                  </div>);
              })
            )}
        </div>
      </div>
    );
  }
}
export default CheckList;
