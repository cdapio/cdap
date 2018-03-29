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
import {connect, Provider} from 'react-redux';
import PropTypes from 'prop-types';
import LoadingSVGCentered from 'components/LoadingSVGCentered';
import NamespaceDetailsStore, {NamespaceDetailsActions} from 'components/NamespaceDetails/store';
import {getData} from 'components/NamespaceDetails/store/ActionCreator';
import TopPanel from 'components/NamespaceDetails/TopPanel';
import Description from 'components/NamespaceDetails/Description';
import EntityCounts from 'components/NamespaceDetails/EntityCounts';
import ComputeProfiles from 'components/NamespaceDetails/ComputeProfiles';
import Preferences from 'components/NamespaceDetails/Preferences';
import Mapping from 'components/NamespaceDetails/Mapping';
import Security from 'components/NamespaceDetails/Security';

require('./NamespaceDetails.scss');

const mapStateToProps = (state) => {
  return {
    loading: state.loading
  };
};

const NamespaceDetailsComp = ({loading}) => {
  if (loading) {
    return <LoadingSVGCentered />;
  }

  return (
    <div className="namespace-details-container">
      <TopPanel />
      <div className="namespace-details-content">
        <Description />
        <hr />
        <EntityCounts />
        <hr />
        <ComputeProfiles />
        <hr className="more-margin-top" />
        <Preferences />
        <hr className="more-margin-top" />
        <Mapping />
        <hr />
        <Security />
      </div>
    </div>
  );
};

NamespaceDetailsComp.propTypes = {
  loading: PropTypes.bool
};

const ConnectedNamespaceDetailsComp = connect(mapStateToProps)(NamespaceDetailsComp);

export default class NamespaceDetails extends Component {
  componentWillMount() {
    getData();
  }

  componentWillUnmount() {
    NamespaceDetailsStore.dispatch({
      type: NamespaceDetailsActions.reset
    });
  }

  render() {
    return (
      <Provider store={NamespaceDetailsStore}>
        <ConnectedNamespaceDetailsComp />
      </Provider>
    );
  }
}
