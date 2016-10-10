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
import {default as NamespaceStore} from 'services/store/store';
import {parseMetadata} from 'services/metadata-parser';
import {
  Col,
  Modal,
  ModalHeader,
  ModalBody,
  Tag,
  Pagination,
  PaginationItem,
  PaginationLink
} from 'reactstrap';

require('./SpotlightModal.less');

const PAGE_SIZE = 10;

export default class SpotlightModal extends Component {
  constructor(props) {
    super(props);

    this.state = {
      searchResults: { results: [] },
      currentPage: 1
    };
  }

  componentWillMount() {
    this.handleSearch(1);
  }

  handleSearch(page) {
    let offset = (page - 1) * PAGE_SIZE;

    MySearchApi.search({
      namespace: NamespaceStore.getState().selectedNamespace,
      query: this.props.query + '*',
      size: PAGE_SIZE,
      offset
    }).subscribe( (res) => {
      this.setState({
        searchResults: res,
        currentPage: page
      });
    });

  }

  handleRenderPagination() {
    const total = this.state.searchResults.total;
    if (!total || total <= PAGE_SIZE) { return null; }

    const numPages = Math.ceil(total / PAGE_SIZE);
    let pageArray = Array.from(Array(numPages).keys()).map( n => n + 1 );

    return (
      <div className="results-pagination text-center">
        <Pagination>
          <PaginationItem disabled={this.state.currentPage === 1}>
            <PaginationLink
              onClick={this.handleSearch.bind(this, this.state.currentPage - 1)}
            >
              <span className="fa fa-chevron-left"></span>
            </PaginationLink>
          </PaginationItem>

          {
            pageArray.map((page) => {
              return (
                <PaginationItem
                  key={page}
                  active={this.state.currentPage === page}
                >
                  <PaginationLink onClick={this.handleSearch.bind(this, page)} >
                    {page}
                  </PaginationLink>
                </PaginationItem>
              );
            })
          }

          <PaginationItem disabled={this.state.currentPage === numPages}>
            <PaginationLink
              onClick={this.handleSearch.bind(this, this.state.currentPage + 1)}
            >
              <span className="fa fa-chevron-right"></span>
            </PaginationLink>
          </PaginationItem>
        </Pagination>
      </div>
    );
  }

  render() {
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
            Search results for: {this.props.query}
          </span>
          <div
            className="close-section pull-right"
          >
            <span className="search-results-total">
              {this.state.searchResults.total} results found
            </span>
            <span
              className="fa fa-times"
              onClick={this.props.toggle}
            />
          </div>
        </ModalHeader>
        <ModalBody>
          <div className="search-results-container">
            {this.state.searchResults.results
              .map(parseMetadata)
              .map((entity, index) => {
                return (
                  <div
                    key={index}
                    className="row search-results-item"
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
                      <div className="entity-tags-container">
                        {
                          entity.metadata.metadata.SYSTEM.tags.map((tag, i) => {
                            return (
                              <Tag key={i}>{tag}</Tag>
                            );
                          })
                        }
                      </div>
                    </Col>
                  </div>
                );
              })
            }
          </div>

          {this.handleRenderPagination()}
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
