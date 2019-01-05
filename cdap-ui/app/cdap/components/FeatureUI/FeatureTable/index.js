/* eslint react/prop-types: 0 */
import React from 'react';
require('./FeatureTable.scss');

class FeatureTable extends React.Component {
  render() {
    let data = this.props.data;
    return (
      <table className='feature-table'>
        <tbody>
          <tr>
            <th>Pipeline</th>
            <th>Status</th>
            <th>Last Run Time</th>
            <th></th>
            <th></th>
            <th></th>
          </tr>
          {data.map(item => {
            return <tr key = {item.pipelineName}>
              <td>{item.pipelineName}</td>
              <td>{item.status}</td>
              <td>{this.getEpochDateString(item.lastStartEpochTime)}</td>
              <td className="center-align">
                <button onClick={this.onView.bind(this, item)}>VIEW</button>
              </td>
              <td className="center-align">
                <button onClick={this.onEdit.bind(this, item)}>EDIT</button>
              </td>
              <td className="center-align">
                <button onClick={this.onDelete.bind(this, item)}>DELETE</button>
              </td>
            </tr>;
          })}
        </tbody>
      </table>
    );
  }
  getEpochDateString(epoch) {
    if(isNaN(epoch)) {
      return "Not Available";
    } else {
      let date = new Date(epoch * 1000);
      return date.toDateString();
    }
  }

  onView(item) {
    if (this.props.onView) {
      this.props.onView(item);
    }
  }

  onEdit(item) {
    if (this.props.onEdit) {
      this.props.onEdit(item);
    }
  }

  onDelete(item) {
    if (this.props.onEdit) {
      this.props.onDelete(item);
    }
  }

}
export default FeatureTable;
