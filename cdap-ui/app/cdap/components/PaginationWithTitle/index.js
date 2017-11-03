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
import ReactPaginate from 'react-paginate';
import React, { Component } from 'react';
require('./PaginationWithTitle.scss');

export default class PaginationWithTitle extends Component {
  static propTypes = {
    currentPage: PropTypes.number,
    totalPages: PropTypes.number,
    title: PropTypes.string,
    numberOfEntities: PropTypes.number,
    handlePageChange: PropTypes.func
  };

  state = {
    title: this.props.title || 'Pages',
    currentPage: this.props.currentPage,
    totalPages: this.props.totalPages,
  }
  render() {
    return (
      <span className="pagination-with-title">
        <ul className="total-entities">
          <span>
            {this.props.numberOfEntities} {this.state.title}
          </span>
        </ul>
        <ReactPaginate
          pageCount={this.props.totalPages}
          pageRangeDisplayed={3}
          marginPagesDisplayed={1}
          breakLabel={<span>...</span>}
          breakClassName={"ellipsis"}
          previousLabel={<span className="fa fa-angle-left"></span>}
          nextLabel={<span className="fa fa-angle-right"></span>}
          onPageChange={this.props.handlePageChange.bind(this)}
          disableInitialCallback={true}
          initialPage={this.props.currentPage-1}
          forcePage={this.props.currentPage-1}
          containerClassName={"page-list"}
          activeClassName={"current-page"}
        />
      </span>
    );
  }
}
