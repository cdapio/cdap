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
import {MySearchApi} from 'api/search';
import NamespaceStore from 'services/NamespaceStore';
import {parseMetadata} from 'services/metadata-parser';
import T from 'i18n-react';
import Mousetrap from 'mousetrap';
import classnames from 'classnames';
import shortid from 'shortid';
import Pagination from 'components/Pagination';
import ReactCSSTransitionGroup from 'react-addons-css-transition-group';

import {
  Dropdown,
  DropdownToggle,
  DropdownMenu,
  DropdownItem,
  Col,
  Modal,
  ModalHeader,
  ModalBody,
  Tag
} from 'reactstrap';

require('./SpotlightModal.less');

const PAGE_SIZE = 10;

export default class SpotlightModal extends Component {
  constructor(props) {
    super(props);

    this.state = {
      searchResults: { results: [] },
      currentPage: 1,
      numPages: 1,
      focusIndex: 0,
      isPaginationExpanded: false,
      animationDirection: 'next'
    };
    this.handlePaginationToggle = this.handlePaginationToggle.bind(this);
  }

  componentWillMount() {
    Mousetrap.bind('right', this.handleNext.bind(this));
    Mousetrap.bind('left', this.handlePrev.bind(this));
    Mousetrap.bind('up', this.handleUpDownArrow.bind(this, 'UP'));
    Mousetrap.bind('down', this.handleUpDownArrow.bind(this, 'DOWN'));
    Mousetrap.bind('enter', this.handleEnter.bind(this));

    this.handleSearch(1);
  }

  componentWillUnmount() {
    Mousetrap.unbind('right');
    Mousetrap.unbind('left');
    Mousetrap.unbind('up');
    Mousetrap.unbind('down');
    Mousetrap.unbind('enter');
  }

  handleNext() {
    this.handleSearch(this.state.currentPage + 1);
  }

  handlePrev() {
    this.handleSearch(this.state.currentPage - 1);
  }

  handleUpDownArrow(direction) {
    if (direction === 'UP') {
      let focusIndex = this.state.focusIndex === 0 ? 0 : this.state.focusIndex - 1;
      this.setState({focusIndex});
    } else if (direction ==='DOWN') {
      let totalResults = this.state.searchResults.results.length;

      let focusIndex = this.state.focusIndex === totalResults - 1 ?
        this.state.focusIndex : this.state.focusIndex + 1;

      this.setState({focusIndex});
    }
  }

  handlePaginationToggle() {
    this.setState({
      isPaginationExpanded: !this.state.isPaginationExpanded
    });
  }

  handleEnter() {
    let entity = parseMetadata(this.state.searchResults.results[this.state.focusIndex]);

    // Redirect to entity once we have entity detail page
    // Right now console logging entity for identification purposes

    console.log('ENTER on:', entity.id);
  }

  handleSearch(page) {
    if (page === 0 || page > this.state.numPages) { return; }

    let offset = (page - 1) * PAGE_SIZE;

    MySearchApi.search({
      namespace: NamespaceStore.getState().selectedNamespace,
      query: this.props.query + '*',
      size: PAGE_SIZE,
      offset
    }).subscribe( (res) => {
      this.setState({
        searchResults: res,
        currentPage: page,
        numPages: Math.ceil(res.total / PAGE_SIZE),
        focusIndex: 0
      });
    });
  }

  render() {
    let bodyContent;

    let searchResultsToBeRendered = (
        this.state.searchResults.results
        .map(parseMetadata)
        .map((entity, index) => {
          return (
            <div
              key={shortid.generate()}
              className={classnames('row search-results-item', {
                active: index === this.state.focusIndex
              })}
            >
              <Col xs="6">
                <div className="entity-title">
                  <span className="entity-icon">
                    <span className={entity.icon} />
                  </span>
                  <span className="entity-name">
                    {entity.id}
                  </span>
                </div>
                <div className="entity-description">
                  <span>
                    {entity.metadata.metadata.SYSTEM.properties.description}
                  </span>
                </div>
              </Col>

              <Col xs="6">
                <div className="entity-tags-container text-right">
                  {
                    entity.metadata.metadata.SYSTEM.tags.map((tag) => {
                      return (
                        <Tag key={shortid.generate()}>{tag}</Tag>
                      );
                    })
                  }
                </div>
              </Col>
            </div>
          );
        })
      );

    bodyContent = () => {
      return (
        <ReactCSSTransitionGroup
          transitionName={"animation--" + this.state.animationDirection}
          transitionEnterTimeout={400}
          transitionLeaveTimeout={400}
        >
          {searchResultsToBeRendered}
        </ReactCSSTransitionGroup>
      );
    };

    let pageArray = Array.from(Array(this.state.numPages).keys()).map( n => n + 1 );

    return (
      <Modal
        isOpen={this.props.isOpen}
        toggle={this.props.toggle}
        className='search-results-modal'
        size="lg"
        backdrop={true}
      >
        <ModalHeader>
          <span className="pull-left">
            {

              T.translate('features.SpotlightSearch.SpotlightModal.headerSearchResults', {
                query: this.props.query
              })
            }
          </span>
          <div
            className="close-section pull-right"
          >
            <span className="search-results-total">
              {
                T.translate('features.SpotlightSearch.SpotlightModal.numResults', {
                  total: this.state.searchResults.total
                })
              }
            </span>
            <span>
            <Dropdown
              isOpen={this.state.isPaginationExpanded}
              toggle={this.handlePaginationToggle}
            >
              <DropdownToggle tag="div">
                <span>Page: {this.state.currentPage}</span>
                <span className="fa fa-caret-down pull-right"></span>
              </DropdownToggle>
              <DropdownMenu>
                {
                  pageArray.map((page, index) => {
                    return (
                      <DropdownItem
                        key={index}
                        onClick={this.handleSearch.bind(this, page)}
                      >
                        {page}
                      </DropdownItem>
                    );
                  })
                }
              </DropdownMenu>
            </Dropdown>
            </span>
            <span
              className="fa fa-times"
              onClick={this.props.toggle}
            />
          </div>
        </ModalHeader>
        <ModalBody>
          <div className="search-results-container">
            <Pagination
              setCurrentPage={this.handleSearch.bind(this)}
              currentPage={this.state.currentPage}
            >
              {bodyContent}
            </Pagination>
          </div>
        </ModalBody>
      </Modal>
    );
  }
}

SpotlightModal.propTypes = {
  query: PropTypes.string,
  isOpen: PropTypes.bool,
  toggle: PropTypes.func
};
