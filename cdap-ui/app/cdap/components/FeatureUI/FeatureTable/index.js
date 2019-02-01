/* eslint react/prop-types: 0 */
import React from 'react';
import { FAILED, DEPLOYED, SUCCEEDED, RUNNING } from '../config';
require('./FeatureTable.scss');

class FeatureTable extends React.Component {
  render() {
    let data = this.props.data;
    return (
      <table className='feature-table'>
        <thead>
          <tr>
            <th>Pipeline Name</th>
            <th>Status</th>
            <th>Last Run Time</th>
            <th>Type</th>
            <th></th>
            <th></th>
          </tr>
        </thead>
        <tbody>
          {data.map(item => {
            return <tr key={item.pipelineName}>
              <td>
                <div className="view-link" onClick={this.onView.bind(this, item)}>
                  {item.pipelineName}</div>
              </td>
              <td>
                <div>
                  <span className={this.getStatusClass(item)}></span>
                  {item.status}
                </div>
              </td>
              <td>{this.getEpochDateString(item.lastStartEpochTime)}</td>
              <td >
                {item.pipelineType}
              </td>
              <td className="center-align">
                {
                  this.isFeatureAvailable(item) &&
                  <button className="feature-button-invert" onClick={this.onFeatureSelection.bind(this, item)}>Feature Selection</button>
                }

              </td>
              <td className="center-align">
                <span className="fa fa-edit right-padding clickable"
                  onClick={this.onEdit.bind(this, item)}></span>
                <span className="fa fa-clone right-padding clickable"
                  onClick={this.onClone.bind(this, item)}></span>
                <span className="fa fa-trash text-danger"
                  onClick={this.onDelete.bind(this, item)}></span>
              </td>
            </tr>;
          })}
        </tbody>
      </table>
    );
  }
  getEpochDateString(epoch) {
    if (isNaN(epoch)) {
      return "—";
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

  onFeatureSelection(item) {
    if (this.props.onFeatureSelection) {
      this.props.onFeatureSelection(item);
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

  onClone(item) {
    if (this.props.onClone) {
      this.props.onClone(item);
    }
  }

  isFeatureAvailable(item) {
    return item && item.status == SUCCEEDED;
  }

  getStatusClass(item) {
    let className = "fa fa-circle right-padding";
    switch (item.status) {
      case SUCCEEDED:
        className += " status-success";
        break;
      case FAILED:
        className += " status-failed";
        break;
      case DEPLOYED:
        className += " status-deployed";
        break;
      case RUNNING:
        className += " status-running";
        break;
    }
    return className;
  }

}
export default FeatureTable;
