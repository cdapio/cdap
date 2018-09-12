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
import PropTypes from 'prop-types';
import {init} from 'components/FieldLevelLineage/store/ActionCreator';
import {Provider} from 'react-redux';
import Store, {Actions} from 'components/FieldLevelLineage/store/Store';
import Lineage from 'components/FieldLevelLineage/Lineage';
import {objectQuery} from 'services/helpers';
import EntityTopPanel from 'components/EntityTopPanel';
import TopPanel from 'components/FieldLevelLineage/TopPanel';

export default class FieldLevelLineage extends Component {
  static propTypes = {
    match: PropTypes.object
  };

  componentDidMount() {
    this.initialize();
  }

  componentWillUnmount() {
    Store.dispatch({
      type: Actions.reset
    });
  }

  componentDidUpdate() {
    this.initialize();
  }

  initialize() {
    const datasetId = objectQuery(this.props, 'match', 'params', 'datasetId');
    init(datasetId);
  }

  render() {
    return (
      <Provider store={Store}>
        <div className="field-level-lineage">
          <EntityTopPanel
            breadCrumbAnchorLink='/'
            breadCrumbAnchorLabel="Back"
            title="Field Level Lineage"
          />

          <TopPanel />

          <Lineage />
        </div>
      </Provider>
    );
  }
}
