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

import React, {PropTypes, Component} from 'react';
import T from 'i18n-react';
import ReactPaginate from 'react-paginate';

require('./EntityListInfo.scss');

export default class EntityListInfo extends Component {
  constructor(props) {
    super(props);
  }
  handlePageChange(data) {
    let clickedIndex = data.selected+1;
    this.props.onPageChange(clickedIndex);
  }

  showPagination() {
    return (
      <span className="pagination">
        {
          this.props.numberOfPages <= 1 ?
            <span className="total-entities">
              {this.props.numberOfEntities} Entities
            </span>
          :
            <span>
              <span className="total-entities">
                {this.props.numberOfEntities}+ Entities
              </span>
              <ReactPaginate
                pageCount={this.props.numberOfPages}
                pageRangeDisplayed={3}
                marginPagesDisplayed={1}
                breakLabel={<span>...</span>}
                breakClassName={"ellipsis"}
                previousLabel={<span className="fa fa-angle-left"></span>}
                nextLabel={<span className="fa fa-angle-right"></span>}
                onPageChange={this.handlePageChange.bind(this)}
                disableInitialCallback={true}
                initialPage={this.props.currentPage-1}
                forcePage={this.props.currentPage-1}
                containerClassName={"page-list"}
                activeClassName={"current-page"}
              />
            </span>
        }
      </span>
    );
  }
  render() {
    let title = T.translate('features.EntityListView.Info.title');

    return (
      <div className={this.props.className}>
        <span className="title">
          <h3>{title} "{this.props.namespace}"</h3>
        </span>
        {
          this.props.numberOfEntities ?
            this.showPagination()
          :
            null
        }
      </div>
    );
  }
}

EntityListInfo.propTypes = {
  className: PropTypes.string,
  namespace: PropTypes.string,
  numberOfPages: PropTypes.number,
  numberOfEntities: PropTypes.number,
  currentPage: PropTypes.number,
  onPageChange: PropTypes.func
};

EntityListInfo.defaultProps = {
  className: '',
  namespace: '',
  numberOfPages: 1,
  numberOfEntities: 0,
  currentPage: 1,
  onPageChange: () => {}
};
