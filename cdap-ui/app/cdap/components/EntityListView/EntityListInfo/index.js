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
import T from 'i18n-react';
import PaginationStepper from 'components/PaginationStepper';
import Typography from '@material-ui/core/Typography';
import NamespaceStore from 'services/NamespaceStore';
import SearchStore from 'components/EntityListView/SearchStore';
import SearchStoreActions from 'components/EntityListView/SearchStore/SearchStoreActions';
import { search, updateQueryString } from 'components/EntityListView/SearchStore/ActionCreator';
import Mousetrap from 'mousetrap';

require('./EntityListInfo.scss');

export default class EntityListInfo extends Component {
  constructor(props) {
    super(props);
  }
  componentWillMount() {
    Mousetrap.bind('right', this.goToNextPage.bind(this));
    Mousetrap.bind('left', this.goToPreviousPage.bind(this));
  }
  componentWillUnmount() {
    Mousetrap.unbind('left');
    Mousetrap.unbind('right');
  }
  goToNextPage() {
    let { currentPage, total, limit } = SearchStore.getState().search;
    if (currentPage === Math.ceil(total / limit)) {
      return;
    }
    SearchStore.dispatch({
      type: SearchStoreActions.SETCURRENTPAGE,
      payload: {
        currentPage: currentPage + 1,
        offset: currentPage * limit,
      },
    });
    search();
    updateQueryString();
  }
  goToPreviousPage() {
    let { currentPage, limit } = SearchStore.getState().search;
    if (currentPage === 1) {
      return;
    }
    SearchStore.dispatch({
      type: SearchStoreActions.SETCURRENTPAGE,
      payload: {
        currentPage: currentPage - 1,
        offset: (currentPage - 2) * limit,
      },
    });
    search();
    updateQueryString();
  }

  showPagination() {
    const { currentPage, total, limit, loading } = SearchStore.getState().search;
    const prevDisabled = currentPage === 1;
    const nextDisabled = currentPage === Math.ceil(total / limit);

    const plus = nextDisabled ? '' : '+';
    const entitiesLabel = T.translate('features.EntityListView.Info.entities');

    return (
      <span className="pagination">
        {loading ? null : (
          <Typography variant="subtitle2" className="total-entities">
            {this.props.numberOfEntities}
            {plus} {entitiesLabel}
          </Typography>
        )}
        {!loading && this.props.numberOfPages > 1 ? (
          <PaginationStepper
            onNext={this.goToNextPage}
            onPrev={this.goToPreviousPage}
            nextDisabled={nextDisabled}
            prevDisabled={prevDisabled}
          />
        ) : null}
      </span>
    );
  }
  render() {
    let namespace = NamespaceStore.getState().selectedNamespace;
    let title = T.translate('features.EntityListView.Info.title', { namespace });

    return (
      <div className={this.props.className}>
        <span className="title">
          <h3 title={namespace}>{title}</h3>
        </span>
        {this.props.numberOfEntities ? this.showPagination() : null}
      </div>
    );
  }
}

EntityListInfo.propTypes = {
  className: PropTypes.string,
  numberOfPages: PropTypes.number,
  numberOfEntities: PropTypes.number,
  currentPage: PropTypes.number,
  allEntitiesFetched: PropTypes.bool,
};

EntityListInfo.defaultProps = {
  className: '',
  numberOfPages: 1,
  numberOfEntities: 0,
  currentPage: 1,
  allEntitiesFetched: false,
};
