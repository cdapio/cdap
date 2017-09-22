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

import PropTypes from 'prop-types';

import React, { Component } from 'react';
import T from 'i18n-react';
import PaginationDropdown from 'components/Pagination/PaginationDropdown';
import {
  ModalHeader
} from 'reactstrap';

require('./SpotlightModal.scss');

export default class SpotlightModalHeader extends Component {
  constructor(props) {
    super(props);
    this.state = {
      isDropdownExpanded : false
    };
    this.toggleExpansion = this.toggleExpansion.bind(this);
  }

  toggleExpansion() {
    this.setState({
      isDropdownExpanded : !this.state.isDropdownExpanded
    });
  }

  render() {
    return (
      <ModalHeader>
        <span className="float-xs-left">
          {
            T.translate('features.SpotlightSearch.SpotlightModal.headerTagResults', {
              tag: this.props.tag
            })
          }
        </span>
        <div
          className="close-section float-xs-right text-xs-right"
        >
          <span className="search-results-total">
            {
             this.props.total === 1 ?
               T.translate('features.SpotlightSearch.SpotlightModal.numResult', {
                total: this.props.total
               })
             :
               T.translate('features.SpotlightSearch.SpotlightModal.numResults', {
                total: this.props.total
               })
            }
          </span>
          <span>
          <PaginationDropdown
           numberOfPages={this.props.numPages}
           currentPage={this.props.currentPage}
           onPageChange={this.props.handleSearch.bind(this)}
          />
          </span>
          <span
            className="fa fa-times"
            onClick={this.props.toggle}
          />
        </div>
      </ModalHeader>
    );

  }
}

SpotlightModalHeader.propTypes = {
  toggle: PropTypes.func,
  handleSearch: PropTypes.func,
  currentPage: PropTypes.number,
  tag: PropTypes.string,
  numPages: PropTypes.number,
  total: PropTypes.number
};
