import React from 'react';
require('./FeatureTable.scss');

class FeatureTable extends React.Component {
  render() {
    let data = this.props.data;
    return (
      <table className = 'feature-table'>
        <tr>
          <th>Pipeline</th>
          <th>Status</th>
          <th>Time</th>
          <th></th>
          <th></th>
          <th></th>
        </tr>
        {data.map(item => {
          return <tr>
            <td>{item.name}</td>
            <td>{item.status}</td>
            <td>{item.time}</td>
            <td>viewPipeline</td>
            <td><button>EDIT</button></td>
            <td>:</td>
          </tr>;
        })}
      </table>
    );
  }
}
export default FeatureTable;