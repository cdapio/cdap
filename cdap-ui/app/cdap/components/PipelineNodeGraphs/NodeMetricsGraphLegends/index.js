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

import PropTypes from 'prop-types';
import React, { Component } from 'react';
import T from 'i18n-react';
import IconSVG from 'components/IconSVG';
import NodeMetricsGraphLegend from 'components/PipelineNodeGraphs/NodeMetricsGraphLegends/NodeMetricsGraphLegend';
require('./NodeMetricsGraphLegends.scss');

const PREFIX = 'features.PipelineSummary.pipelineNodesMetricsGraph';

// Need this because we want to show Errors separately instead of grouped together with Records Out/ports
const recordsErrorTitle = T.translate(`${PREFIX}.recordsErrorTitle`);

export default class NodeMetricsGraphLegends extends Component {
  static propTypes = {
    items: PropTypes.arrayOf(
      PropTypes.oneOfType([
        PropTypes.shape({
          title: PropTypes.string.isRequired,
          color: PropTypes.string,
          disabled: PropTypes.bool
        }),
        PropTypes.string.isRequired,
        PropTypes.element
      ])
    ).isRequired,
    checkedItems: PropTypes.arrayOf(
      PropTypes.string
    ),
    onLegendClick: PropTypes.func,
    isMultiplePorts: PropTypes.bool
  };

  state = {
    showLegendPopover: false
  };

  itemsWithoutErrorRecords = this.props.items.filter((item) => item.title !== recordsErrorTitle);

  toggleLegendPopover = () => {
    this.setState({
      showLegendPopover: !this.state.showLegendPopover
    });
  };

  renderRecordsErrorLegend(showCheckbox = false, orientation = 'horizontal') {
    let recordsErrorItem = this.props.items.find(item => item.title === recordsErrorTitle);
    if (!recordsErrorItem) {
      return null;
    }

    let itemChecked = this.props.checkedItems.indexOf(recordsErrorItem.title) !== -1;

    return (
      <NodeMetricsGraphLegend
        item={recordsErrorItem}
        showCheckbox={showCheckbox}
        orientation={orientation}
        itemChecked={itemChecked}
        onLegendClick={this.props.onLegendClick}
      />
    );
  }

  renderNormalMetricsLegends() {
    return (
      <div className='rv-discrete-color-legend horizontal'>
        {
          this.itemsWithoutErrorRecords
            .map((item, i) =>
              <NodeMetricsGraphLegend
                item={item}
                key={i}
              />
            )
        }
        {this.renderRecordsErrorLegend()}
      </div>
    );
  }

  renderPortsMetricsLegends() {
    return (
      <div className='rv-discrete-color-legend horizontal'>
        {
          this.itemsWithoutErrorRecords
            .map((item, i) => {
              let itemChecked = this.props.checkedItems.indexOf(item.title) !== -1;

              return (
                <NodeMetricsGraphLegend
                  item={item}
                  key={i}
                  showCheckbox={true}
                  itemChecked={itemChecked}
                  onLegendClick={this.props.onLegendClick}
                />
              );
            })
        }
        {this.renderRecordsErrorLegend(true)}
      </div>
    );
  }

  renderPortsMetricsLegendsPopover() {
    let checkedItemsWithoutErrorRecords = this.props.checkedItems.filter((item) => item !== recordsErrorTitle);
    let itemToShowOnTop = this.itemsWithoutErrorRecords[0];
    if (checkedItemsWithoutErrorRecords.length) {
      itemToShowOnTop = this.itemsWithoutErrorRecords.find(item => item.title === checkedItemsWithoutErrorRecords[0]);
    }
    let showCheckedItemsCount = this.state.showLegendPopover || checkedItemsWithoutErrorRecords.length === 0 || checkedItemsWithoutErrorRecords.length >= 2;

    return (
      <div className='ports-legend-popover-with-errors'>
        <div className='ports-legend-popover'>
          <div
            className="legend-popover-toggle"
            onClick={this.toggleLegendPopover}
          >
            {
              showCheckedItemsCount ?
                (
                  <span>
                    {
                      T.translate(`${PREFIX}.checkedPortLegendsCount`, {
                        selected: checkedItemsWithoutErrorRecords.length,
                        total: this.itemsWithoutErrorRecords.length
                      })
                    }
                  </span>
                )
              :
                (
                  <div className='rv-discrete-color-legend horizontal'>
                    <NodeMetricsGraphLegend item={itemToShowOnTop} />
                  </div>
                )
            }
            <span className="toggle-caret">
              <IconSVG name="icon-caret-down" />
            </span>
          </div>
          {
            !this.state.showLegendPopover ?
              null
            :
              (
                <div className='legend-popover'>
                  <div className='popover-content rv-discrete-color-legend'>
                    {
                      this.itemsWithoutErrorRecords
                        .map((item, i) => {
                          let itemChecked = this.props.checkedItems.indexOf(item.title) !== -1;
                          return (
                            <NodeMetricsGraphLegend
                              item={item}
                              key={i}
                              showCheckbox={true}
                              itemChecked={itemChecked}
                              onLegendClick={this.props.onLegendClick}
                              orientation='vertical'
                            />
                          );
                        })
                    }
                  </div>
                </div>
              )
          }
        </div>
        <div className='rv-discrete-color-legend horizontal error-item'>
          {this.renderRecordsErrorLegend(true)}
        </div>
      </div>
    );
  }

  render() {
    if (!this.props.isMultiplePorts) {
      return this.renderNormalMetricsLegends();
    } else if (this.props.isMultiplePorts && this.itemsWithoutErrorRecords.length <= 2) {
      return this.renderPortsMetricsLegends();
    }
    return this.renderPortsMetricsLegendsPopover();
  }
}
