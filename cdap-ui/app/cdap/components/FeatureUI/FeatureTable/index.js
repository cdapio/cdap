/* eslint react/prop-types: 0 */
import React from 'react';
import { COMPLETED, FAILED, DEPLOYED } from '../config';
require('./FeatureTable.scss');

class FeatureTable extends React.Component {
  render() {
    let data = this.props.data;
    return (
      <table className='feature-table'>
        <thead>
          <tr>
            <th>Pipeline</th>
            <th>Status</th>
            <th>Last Run Time</th>
            <th></th>
            <th></th>
            <th></th>
          </tr>
        </thead>
        <tbody>
          {data.map(item => {
            return <tr key = {item.pipelineName}>
              <td>{item.pipelineName}</td>
              <td>
                <div>
                  <span className = {this.getStatusClass(item)}></span>
                  {item.status}
                </div>
              </td>
              <td>{this.getEpochDateString(item.lastStartEpochTime)}</td>
              <td >
                <div className = {(this.isViewable(item))? "view-link": "disable-link"}
                  onClick={this.onView.bind(this, item)}>
                  VIEW PIPELINE
                </div>
              </td>
              <td className="center-align">
                {
                  this.isViewable(item) &&
                    <button onClick={this.onFeatureSelection.bind(this, item)}>Feature Selection</button>
                }

              </td>
              <td className="center-align">
                <span className = "fa fa-edit right-padding clickable"
                    onClick={this.onEdit.bind(this, item)}></span>
                <span className = "fa fa-clone right-padding clickable"
                    onClick={this.onClone.bind(this, item)}></span>
                <span className = "fa fa-trash text-danger"
                  onClick={this.onDelete.bind(this, item)}></span>
              </td>
            </tr>;
          })}
        </tbody>
      </table>
    );
  }
  getEpochDateString(epoch) {
    if(isNaN(epoch)) {
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

  isViewable(item) {
    return item && item.status == COMPLETED;
  }

  getStatusClass(item) {
    let className = "fa fa-circle right-padding";
    switch(item.status) {
      case COMPLETED:
        className += " status-success";
        break;
      case FAILED:
        className += " status-failed";
        break;
      case DEPLOYED:
        className += " status-deployed";
        break;
    }
    return className;
  }

}
export default FeatureTable;
