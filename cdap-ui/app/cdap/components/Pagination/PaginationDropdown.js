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

import React, {Component, PropTypes} from 'react';
import { Dropdown, DropdownToggle, DropdownMenu, DropdownItem } from 'reactstrap';
import T from 'i18n-react';
import shortid from 'shortid';

require('./Pagination.less');

export default class PaginationDropdown extends Component {
  constructor(props){
    super(props);
    this.state = {
      isPaginationExpanded : false
    };
    this.handleExpansionToggle = this.handleExpansionToggle.bind(this);
  }

  handleExpansionToggle() {
    this.setState({
      isPaginationExpanded : !this.state.isPaginationExpanded
    });
  }

  render() {
    let dropdownItems = [];

    for(let i = 0; i < this.props.numberOfPages; i++){
      dropdownItems.push(
        <div className="dropdownItems">
          {i + 1}
          {
            this.props.currentPage === (i + 1) ?
            <span className="fa fa-check pull-right"></span> :
            null
          }
        </div>
      );
    }

    return (
      <Dropdown
        isOpen={this.state.isPaginationExpanded}
        toggle={this.handleExpansionToggle}
      >
        <DropdownToggle tag="div">
          <span>{T.translate('features.Pagination.dropdown-label')}</span>
          <span className="current-page">{this.props.currentPage}</span>
          <span className="fa fa-caret-down pull-right"></span>
        </DropdownToggle>
        <DropdownMenu onClick={e => e.stopPropagation()}>
          {
            dropdownItems.map((item, index) => {
              return (
                <DropdownItem
                  key={shortid.generate()}
                  onClick={this.props.onPageChange.bind(this, index+1)}
                >
                  {item}
                </DropdownItem>
              );
            })
          }
        </DropdownMenu>
      </Dropdown>
    );
  }
}

PaginationDropdown.propTypes = {
  numberOfPages: PropTypes.number,
  currentPage: PropTypes.number,
  onPageChange: PropTypes.func
};
