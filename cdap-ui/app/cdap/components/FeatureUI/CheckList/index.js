/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

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
      props.dataProvider.map((item) => {
        if (item.checked) {
          checkedItems.set(item.name, item.checked);
        }
      });
    }
    return checkedItems;
  }

  onItemClick(name, event) {
    const isChecked = event.target.checked;
    if (this.props.isSingleSelect) {
      let checkedItem = new Map();
      checkedItem.set(name, isChecked);
      this.setState({ checkedItems: checkedItem });
    } else {
      this.setState(prevState => ({ checkedItems: prevState.checkedItems.set(name, isChecked) }));
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
              listData.map((item) => {
                return (
                  <div className='list-item' key={item.name}>
                    <label className='check-box-container'>
                      <input type="checkbox"
                      checked = {this.state.checkedItems.get(item.name) || false}
                      onChange={this.onItemClick.bind(this, item.name)} />
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
