
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

import React, { Component } from 'react';
import { isNil, cloneDeep } from 'lodash';
import PropTypes from 'prop-types';
import StatusItem from "./StatusItem";
import './StatusBar.scss';

class StatusBar extends Component {
  constructor(props) {
    super(props);
    this.state = {
      statusList: props.statusList,
      featureTypes: props.featureTypes,
      totalCount: this.getTotalCount(props.statusList),
      totalSelected: true,
      totalTypeSelected: true,
    };
  }

  getTotalCount = (list) => {
    let count = 0;
    if (!isNil(list)) {
      list.forEach(element => {
        count += element.count;
      });
    }
    return count;
  }

  statusItemClicked = (status) => {
    this.updateStateSelection(status, false);
  }

  allStatusClicked = () => {
    this.updateStateSelection(null, true);
  }

  updateStateSelection = (item, isAll = false) => {
    let statusList = cloneDeep(this.state.statusList);
    statusList.forEach(element => {
      if (isAll) {
        element.selected = false;
      } else {
        element.selected = element.name === item.name ? true : false;
      }
    });

    if (isAll) {
      this.setState({ statusList: statusList, totalSelected: true });
      this.props.statusSelectionChange("All");
    } else {
      this.setState({ statusList: statusList, totalSelected: false });
      this.props.statusSelectionChange(item.name);
    }
  }


  typeItemClicked = (type) => {
    this.updateTypeSelection(type, false);
  }

  allTypeClicked = () => {
    this.updateTypeSelection(null, true);
  }

  updateTypeSelection = (item, isAll = false) => {
    let typeList = cloneDeep(this.state.featureTypes);
    typeList.forEach(element => {
      if (isAll) {
        element.selected = false;
      } else {
        element.selected = element.name === item.name ? true : false;
      }
    });

    if (isAll) {
      this.setState({ featureTypes: typeList, totalTypeSelected: true });
      this.props.pipeLineSelectionTypeChange('All');
    } else {
      this.setState({ featureTypes: typeList, totalTypeSelected: false });
      this.props.pipeLineSelectionTypeChange(item.name);
    }
  }



  render() {
    return (
      <div className="status-bar-box">
           {
              this.props.statusList.map((status) => {
                return (
                  <StatusItem item={status} key={'status_' + status.id.toString()} ></StatusItem>
                );
              })
            }
        </div>
    );
  }
}

export default StatusBar;
StatusBar.propTypes = {
  statusList: PropTypes.array,
  featureTypes: PropTypes.array,
  statusSelectionChange: PropTypes.func,
  pipeLineSelectionTypeChange: PropTypes.func
};
