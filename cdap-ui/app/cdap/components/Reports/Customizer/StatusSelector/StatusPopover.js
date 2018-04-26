/*
 * Copyright © 2018 Cask Data, Inc.
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
import PropTypes from 'prop-types';
import Popover from 'components/Popover';
import IconSVG from 'components/IconSVG';
import {connect} from 'react-redux';
import {ReportsActions} from 'components/Reports/store/ReportsStore';
import StatusViewer from 'components/Reports/Customizer/StatusSelector/StatusViewer';

const OPTIONS = [
  'FAILED',
  'COMPLETED',
  'RUNNING',
  'STOPPED'
];

class StatusPopoverView extends Component {
  static propTypes = {
    selections: PropTypes.array,
    onApply: PropTypes.func
  };

  state = {
    selections: this.props.selections
  };

  componentWillReceiveProps(nextProps) {
    this.setState({
      selections: nextProps.selections
    });
  }

  isSelected = (option) => {
    return this.state.selections.indexOf(option) !== -1;
  };

  toggleOption = (option) => {
    let index = this.state.selections.indexOf(option);

    let newArr = [...this.state.selections];

    if (index === -1) {
      newArr.push(option);
    } else {
      newArr.splice(index, 1);
    }

    this.setState({
      selections: newArr
    });
  };

  apply = () => {
    this.props.onApply(this.state.selections);

    // Closing popover
    document.body.click();
  };

  render() {
    return (
      <Popover
        target={StatusViewer}
        className="status-selector-popover"
        placement="bottom"
        bubbleEvent={false}
        enableInteractionInPopover={true}
        injectOnToggle={true}
      >
        <div className="options">
          {
            OPTIONS.map((option) => {
              return (
                <div
                  className="option"
                  onClick={this.toggleOption.bind(this, option)}
                >
                  <IconSVG name={this.isSelected(option) ? 'icon-check-square' : 'icon-square-o' } />
                  <IconSVG name="icon-circle" className={option.toLowerCase()} />

                  <span>
                    {option}
                  </span>
                </div>
              );
            })
          }
        </div>

        <div className="action">
          <button
            className="btn btn-link"
            onClick={this.apply}
          >
            Apply
          </button>
        </div>
      </Popover>
    );
  }
}

const mapStateToProps = (state) => {
  return {
    selections: state.status.statusSelections
  };
};

const mapDispatch = (dispatch) => {
  return {
    onApply: (statusSelections) => {
      dispatch({
        type: ReportsActions.setStatus,
        payload: {
          statusSelections
        }
      });
    }
  };
};

const StatusPopover = connect(
  mapStateToProps,
  mapDispatch
)(StatusPopoverView);

export default StatusPopover;
