/* eslint react/prop-types: 0 */
import React from 'react';
import isEmpty from 'lodash/isEmpty';

require("./List.scss");

class List extends React.Component {
  constructor(props) {
    super(props);
  }

  render() {
    let listData = isEmpty(this.props.dataProvider) ? [] : this.props.dataProvider;
    return (
      <div>
        {
          this.props.header &&
          <div className={this.props.headerClass} key={this.props.header}
            onClick={this.props.onHeaderClick}>{this.props.header}
          </div>
        }
        {
          listData.map(item => {
            return (
              <div className="schema-list-item" key={item}>
                <div className="parent-item">{item.parent+ ": "} </div>
                <div className="child-item">{item.child}</div>
              </div>);
          })
        }
      </div>);
  }
}
export default List;
