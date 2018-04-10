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

import React, {Component} from 'react';
import classnames from 'classnames';
import PropTypes from 'prop-types';
import uuidV4 from 'uuid/v4';
import IconSVG from 'components/IconSVG';
import orderBy from 'lodash/orderBy';
import {objectQuery} from 'services/helpers';
import isEqual from 'lodash/isEqual';

require('./SortableStickyGrid.scss');

const SORT_ORDERS = {
  asc: 'asc',
  desc: 'desc'
};

export default class SortableStickyGrid extends Component {
  constructor(props) {
    super(props);
    let sortProperty = objectQuery(props.gridHeaders, 0, 'property');
    let sortOrder = SORT_ORDERS.asc;

    this.state = {
      entities: orderBy(props.entities, [sortProperty], [sortOrder]),
      sortProperty,
      sortOrder
    };
  }

  static propTypes = {
    entities: PropTypes.arrayOf(PropTypes.object).isRequired,
    gridHeaders: PropTypes.arrayOf(
      PropTypes.shape({
        label: PropTypes.string,
        property: PropTypes.string
      })
    ),
    renderGridHeader: PropTypes.func,
    renderGridBody: PropTypes.func,
    className: PropTypes.string,
    cellIsClickable: PropTypes.bool
  };

  static defaultProps = {
    cellIsClickable: false
  };

  componentWillReceiveProps(nextProps) {
    if (!isEqual(this.props.entities, nextProps.entities)) {
      this.setState({
        entities: orderBy(nextProps.entities, [this.state.sortProperty], [this.state.sortOrder])
      });
    }
  }

  componentDidUpdate() {
    let highlightedElems = document.getElementsByClassName('highlighted');
    if (highlightedElems.length) {
      highlightedElems[0].scrollIntoView();
    }
  }

  handleSort = (property) => {
    let newSortProperty, newSortOrder;
    if (this.state.sortProperty === property) {
      newSortProperty = this.state.sortProperty;
      newSortOrder = this.state.sortOrder === SORT_ORDERS.asc ? SORT_ORDERS.desc : SORT_ORDERS.asc;
    } else {
      newSortProperty = property;
      newSortOrder = SORT_ORDERS.asc;
    }

    this.setState({
      sortProperty: newSortProperty,
      sortOrder: newSortOrder,
      entities: orderBy(this.state.entities, [newSortProperty], [newSortOrder])
    });
  };

  renderSortIcon(property) {
    if (property !== this.state.sortProperty) {
      return null;
    }

    return (
      this.state.sortOrder === SORT_ORDERS.asc ?
        <IconSVG name="icon-caret-down" />
      :
        <IconSVG name="icon-caret-up" />
    );
  }


  renderGridHeader() {
    if (this.props.renderGridHeader) {
      return this.props.renderGridHeader();
    }

    return (
      <div className="grid-header">
        <div className="grid-row">
          {
            this.props.gridHeaders.map((header) => {
              if (header.property) {
                return (
                  <strong
                    className={classnames("sortable-header", {"active": this.state.sortProperty === header.property})}
                    key={uuidV4()}
                    onClick={this.handleSort.bind(this, header.property)}
                  >
                    <span>{header.label}</span>
                    {this.renderSortIcon(header.property)}
                  </strong>
                );
              }
              return (
                <strong key={uuidV4()}>
                  {header.label}
                </strong>
              );
            })
          }
        </div>
      </div>
    );
  }

  renderGridBody() {
    if (this.props.renderGridBody) {
      return this.props.renderGridBody(this.state.entities);
    }

    return (
      <div className="grid-body">
        {
          this.state.entities.map((entity) => {
            return (
              <div
                className={classnames(
                  "grid-row", {
                    "grid-link": this.props.cellIsClickable,
                    "highlighted": entity.highlighted
                  }
                )}
                key={uuidV4()}
              >
                {
                  this.props.gridHeaders.map((header) => {
                    return (
                      <div key={uuidV4()}>
                        {entity[header.property]}
                      </div>
                    );
                  })
                }
              </div>
            );
          })
        }
      </div>
    );
  }

  render() {
    let gridClasses = classnames('grid-wrapper sortable-sticky-grid', this.props.className);
    return (
      <div className={gridClasses}>
        <div className="grid grid-container">
          {this.renderGridHeader()}
          {this.renderGridBody()}
        </div>
      </div>
    );
  }
}
