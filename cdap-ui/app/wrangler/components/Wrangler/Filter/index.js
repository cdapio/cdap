/*
 * Copyright © 2016 Cask Data, Inc.
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

import React, { Component, PropTypes } from 'react';
import WranglerStore from 'wrangler/components/Wrangler/Store/WranglerStore';
import WranglerActions from 'wrangler/components/Wrangler/Store/WranglerActions';
import classnames from 'classnames';
import {Tooltip} from 'reactstrap';
import T from 'i18n-react';

export default class Filter extends Component {
  constructor(props) {
    super(props);

    this.state = {
      showFilter: false,
      filterIgnoreCase: false,
      filterFunction: '=',
      tooltipOpen: false
    };

    this.onFilterClick = this.onFilterClick.bind(this);
    this.onFilter = this.onFilter.bind(this);
    this.toggleTooltip = this.toggleTooltip.bind(this);
    this.handleKeyPress = this.handleKeyPress.bind(this);
  }

  toggleTooltip() {
    this.setState({tooltipOpen: !this.state.tooltipOpen});
  }

  onFilterClick() {
    if (!this.props.column) { return; }
    this.setState({showFilter: !this.state.showFilter});
  }

  handleKeyPress(e) {
    if (e.nativeEvent.keyCode === 13) {
      this.onFilter();
    }
  }

  renderFilter() {
    if (!this.state.showFilter || !this.props.column) { return null; }

    return (
      <div
        className="filter-input"
        onClick={e => e.stopPropagation()}
      >
        <select
          className="form-control"
          onChange={e => this.setState({filterFunction: e.target.value})}
          value={this.state.filterFunction}
        >
          <option value="=">{T.translate('features.Wrangler.Filter.Options.equal')}</option>
          <option value="!=">{T.translate('features.Wrangler.Filter.Options.doesNotEqual')}</option>
          <option value="<">{T.translate('features.Wrangler.Filter.Options.lessThan')}</option>
          <option value=">">{T.translate('features.Wrangler.Filter.Options.greaterThan')}</option>
          <option value="<=">{T.translate('features.Wrangler.Filter.Options.lessEqual')}</option>
          <option value=">=">{T.translate('features.Wrangler.Filter.Options.greaterEqual')}</option>
          <option value="startsWith">{T.translate('features.Wrangler.Filter.Options.startsWith')}</option>
          <option value="endsWith">{T.translate('features.Wrangler.Filter.Options.endsWith')}</option>
          <option value="contains">{T.translate('features.Wrangler.Filter.Options.contains')}</option>
        </select>

        <div>
          <input
            type="text"
            className="form-control"
            onChange={(e) => this.filterByText = e.target.value}
            onKeyPress={this.handleKeyPress}
            placeholder={T.translate('features.Wrangler.Filter.filter')}
          />
        </div>

        <div className="checkbox form-check">
          <label className="control-label form-check-label">
            <input
              type="checkbox"
              className="form-check-input"
              checked={this.state.filterIgnoreCase}
              onChange={() => this.setState({filterIgnoreCase: !this.state.filterIgnoreCase})}
            />
            {T.translate('features.Wrangler.Filter.ignoreCase')}
          </label>
        </div>
        <br/>
        <div className="text-xs-right">
          <button
            className="btn btn-wrangler"
            onClick={this.onFilter}
          >
            {T.translate('features.Wrangler.Filter.apply')}
          </button>
        </div>
      </div>
    );
  }

  onFilter() {
    if (!this.filterByText) {
      this.setState({filter: null});
      WranglerStore.dispatch({
        type: WranglerActions.setFilter,
        payload: {
          filter: null
        }
      });
    } else {
      let filterObj = {
        column: this.props.column,
        filterBy: this.filterByText,
        filterFunction: this.state.filterFunction,
        filterIgnoreCase: this.state.filterIgnoreCase
      };

      this.setState({
        filter: filterObj
      });

      WranglerStore.dispatch({
        type: WranglerActions.setFilter,
        payload: {
          filter: filterObj
        }
      });
    }
  }

  render() {
    const id = 'wrangler-filter-transform';

    return (
      <div
        className={classnames('transform-item', {
          disabled: !this.props.column,
          expanded: this.state.showFilter
        })}
        onClick={this.onFilterClick}
      >
        <span className="fa fa-font"></span>
        <span
          id={id}
          className="transform-item-text"
        >
          {T.translate('features.Wrangler.Filter.filter')}
        </span>

        {
          !this.props.column ? (
            <Tooltip
              placement="right"
              isOpen={this.state.tooltipOpen}
              toggle={this.toggleTooltip}
              target={id}
              className="wrangler-tooltip"
              delay={{show: 300, hide: 0}}
              tether={{offset: '0 -10px'}}
            >
              {T.translate('features.Wrangler.LeftPanel.selectColumn', {type: 'filter'})}
            </Tooltip>
          ) : null
        }


        {this.renderFilter()}
      </div>
    );
  }
}

Filter.propTypes = {
  column: PropTypes.string
};
