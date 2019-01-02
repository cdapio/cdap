import React from 'react';
import isEmpty from 'lodash/isEmpty';


class List extends React.Component {
  constructor(props) {
    super(props);
  }

  render() {
    let listData = isEmpty(this.props.dataProvider) ? []: this.props.dataProvider;
    console.log("Rendering list ", listData);
    return (
      <div>
        {
          this.props.header &&
          <h3 onClick = {this.props.onHeaderClick}>this.props.header
          </h3>
        }
        {
          listData.map(item => <div>{item}</div>)
        }
      </div>
    );
  }
}
export default List;