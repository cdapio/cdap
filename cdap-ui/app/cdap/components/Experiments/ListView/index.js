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

import React, { Component } from 'react';
import Helmet from 'react-helmet';
import ExperimentsListViewWrapper from 'components/Experiments/ListView/ListViewWrapper';
import { Provider } from 'react-redux';
import experimentsStore, { DEFAULT_EXPERIMENTS } from 'components/Experiments/store';
import { getExperimentsList, setAlgorithmsList, updatePagination, handlePageChange } from 'components/Experiments/store/ActionCreator';
import queryString from 'query-string';
import isNil from 'lodash/isNil';
import Mousetrap from 'mousetrap';

export default class ExperimentsList extends Component {
  componentWillMount() {
    setAlgorithmsList();
    Mousetrap.bind('right', this.goToNextPage);
    Mousetrap.bind('left', this.goToPreviousPage);
    this.parseUrlAndUpdateStore();
    getExperimentsList();
  }

  componentWillUnmount() {
    Mousetrap.unbind('left');
    Mousetrap.unbind('right');
  }

  componentWillReceiveProps(nextProps) {
    this.parseUrlAndUpdateStore(nextProps);
    getExperimentsList();
  }

  goToNextPage = () => {
    let {offset, limit, totalPages} = experimentsStore.getState().experiments;
    let nextPage = offset === 0 ? 1 : Math.ceil((offset + 1) / limit);
    if (nextPage < totalPages) {
      handlePageChange({ selected: nextPage });
    }
  };

  goToPreviousPage = () => {
    let {offset, limit} = experimentsStore.getState().experiments;
    let prevPage = offset === 0 ? 1 : Math.ceil((offset + 1) / limit);
    if (prevPage > 1) {
      handlePageChange({ selected: prevPage - 2 });
    }
  };

  parseUrlAndUpdateStore = (nextProps) => {
    let props = nextProps || this.props;
    let { offset, limit } = this.getQueryObject(queryString.parse(props.location.search));
    updatePagination({ offset, limit });
  };

  getQueryObject = (query) => {
    if (isNil(query)) {
      return {};
    }
    let {
      offset = DEFAULT_EXPERIMENTS.offset,
      limit = DEFAULT_EXPERIMENTS.limit
    } = query;
    offset = parseInt(offset, 10);
    limit = parseInt(limit, 10);
    if (isNaN(offset)) {
      offset = DEFAULT_EXPERIMENTS.offset;
    }
    if (isNaN(limit)) {
      limit = DEFAULT_EXPERIMENTS.limit;
    }
    return { offset, limit };
  };

  render() {
    return (
      <Provider store={experimentsStore}>
        <div>
          <Helmet title="CDAP | All Experiments" />
          <ExperimentsListViewWrapper />
        </div>
      </Provider>
    );
  }
}
