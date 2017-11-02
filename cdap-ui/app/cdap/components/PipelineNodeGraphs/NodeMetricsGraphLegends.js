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
import classnames from 'classnames';
import IconSVG from 'components/IconSVG';

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
    onLegendClick: PropTypes.function,
    isMultiplePorts: PropTypes.bool
  };

  state = {
    showLegendPopover: false
  };

  toggleLegendPopover = () => {
    this.setState({
      showLegendPopover: !this.state.showLegendPopover
    });
  };

  renderNormalMetricsLegends() {
    return (
      <div className='rv-discrete-color-legend horizontal'>
        {
          this.props.items
            .map((item, i) => {
              return (
                <div
                  key={i}
                  className="rv-discrete-color-legend-item horizontal"
                >
                  <span
                    className="rv-discrete-color-legend-item__color"
                    style={{background: item.color}}
                  />
                  <span className="rv-discrete-color-legend-item__title">
                    {item.title}
                  </span>
                </div>
              );
            })
        }
      </div>
    );
  }

  renderPortsMetricsLegendsPopover() {
    return (
      <div className='ports-legend-popover'>
        <div
          className="legend-popover-toggle"
          onClick={this.toggleLegendPopover}
        >
          {
            this.state.showLegendPopover ?
              (
                <span>
                  {`${this.props.checkedItems.length} of ${this.props.items.length} Metrics Displayed`}
                </span>
              )
            :
              (
                <div className='rv-discrete-color-legend horizontal'>
                  <div className="rv-discrete-color-legend-item horizontal">
                    <span
                      className="rv-discrete-color-legend-item__color"
                      style={{background: this.props.items[0].color}}
                    />
                    <span className="rv-discrete-color-legend-item__title">
                      {this.props.items[0].title}
                    </span>
                  </div>
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
                    this.props.items
                      .map((item, i) => {
                        let itemChecked = this.props.checkedItems.indexOf(item.title) !== -1;
                        return (
                          <div
                            key={i}
                            className="rv-discrete-color-legend-item vertical pointer"
                            onClick={this.props.onLegendClick.bind(null, item.title)}
                          >
                            <span className={classnames('fa legend-item-checkbox', {
                                'fa-square-o': !itemChecked,
                                'fa-check-square': itemChecked
                              })}
                            />
                            <span
                              className="rv-discrete-color-legend-item__color"
                              style={{background: item.color}}
                            />
                            <span className="rv-discrete-color-legend-item__title">
                              {item.title}
                            </span>
                          </div>
                        );
                      })
                  }
                </div>
              </div>
            )
        }
      </div>
    );
  }

  renderPortsMetricsLegends() {
    return (
      <div className='rv-discrete-color-legend horizontal'>
        {
          this.props.items
            .map((item, i) => {
              let itemChecked = this.props.checkedItems.indexOf(item.title) !== -1;
              return (
                <div
                  key={i}
                  className="rv-discrete-color-legend-item horizontal pointer"
                  onClick={this.props.onLegendClick.bind(null, item.title)}
                >
                  <span className={classnames('fa legend-item-checkbox', {
                      'fa-square-o': !itemChecked,
                      'fa-check-square': itemChecked
                    })}
                  />
                  <span
                    className="rv-discrete-color-legend-item__color"
                    style={{background: item.color}}
                  />
                  <span className="rv-discrete-color-legend-item__title">
                    {item.title}
                  </span>
                </div>
              );
            })
        }
      </div>
    );
  }

  render() {
    if (!this.props.isMultiplePorts) {
      return this.renderNormalMetricsLegends();
    } else if (this.props.isMultiplePorts && this.props.items.length > 2) {
      return this.renderPortsMetricsLegendsPopover();
    }
    return this.renderPortsMetricsLegends();
  }
}
