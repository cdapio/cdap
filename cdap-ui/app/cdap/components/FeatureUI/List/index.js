import React from 'react';
import isEmpty from 'lodash/isEmpty';


class List extends React.Component {
  constructor(props) {
    super(props);
  }

  render() {
    let listData = isEmpty(this.props.dataProvider) ? []: this.props.dataProvider;
    console.log("Rendering list ", listData, this.props.header, this.props.headerClass);
    return (
      <div>
        {
          this.props.header &&
          <div className = {this.props.headerClass} key = {this.props.header}
            onClick = {this.props.onHeaderClick}>{this.props.header}
          </div>
        }
        {
          listData.map(item => <div>{item}</div>)
        }
      </div>
    );
  }
}
export default List;