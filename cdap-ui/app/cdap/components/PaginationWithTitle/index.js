/*
 * Copyright © 2017-2018 Cask Data, Inc.
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
import ReactPaginate from 'react-paginate';
import React, { Component } from 'react';
require('./PaginationWithTitle.scss');

export default class PaginationWithTitle extends Component {
  static propTypes = {
    currentPage: PropTypes.number,
    totalPages: PropTypes.number,
    title: PropTypes.string,
    handlePageChange: PropTypes.func,
  };

  renderPaginationComponent() {
    if (this.props.totalPages < 2) {
      return null;
    }

    return (
      <ReactPaginate
        pageCount={this.props.totalPages}
        pageRangeDisplayed={3}
        marginPagesDisplayed={1}
        breakLabel={<span>...</span>}
        breakClassName={'ellipsis'}
        previousLabel={<span className="fa fa-angle-left" />}
        nextLabel={<span className="fa fa-angle-right" />}
        onPageChange={this.props.handlePageChange.bind(this)}
        disableInitialCallback={true}
        initialPage={this.props.currentPage - 1}
        forcePage={this.props.currentPage - 1}
        containerClassName={'page-list'}
        activeClassName={'current-page'}
      />
    );
  }

  render() {
    return (
      <span className="pagination-with-title">
        <ul className="total-entities">
          <span>{this.props.title}</span>
        </ul>
        {this.renderPaginationComponent()}
      </span>
    );
  }
}
