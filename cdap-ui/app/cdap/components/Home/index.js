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
import React, {PropTypes, Component} from 'react';
import Match from 'react-router/Match';
import Miss from 'react-router/Miss';
import Page404 from 'components/404';
import EntityListView from 'components/EntityListView';
import AppDetailedView from 'components/AppDetailedView';
import DatasetDetailedView from 'components/DatasetDetailedView';
import StreamDetailedView from 'components/StreamDetailedView';
import NamespaceStore from 'services/NamespaceStore';
import NamespaceActions from 'services/NamespaceStore/NamespaceActions';
import DataPrepHome from 'components/DataPrepHome';
import FileBrowser from 'components/FileBrowser';

export default class Home extends Component {
  componentWillMount() {
    NamespaceStore.dispatch({
      type: NamespaceActions.selectNamespace,
      payload: {
        selectedNamespace: this.props.params.namespace
      }
    });
  }
  render() {
    return (
      <div>
        <Match exactly pattern="/ns/:namespace" component={EntityListView} />
        <Match pattern="/ns/:namespace/apps/:appId" component={AppDetailedView} />
        <Match pattern="/ns/:namespace/datasets/:datasetId" component={DatasetDetailedView} />
        <Match pattern="/ns/:namespace/streams/:streamId" component={StreamDetailedView} />
        <Match pattern="/ns/:namespace/dataprep" component={DataPrepHome} />
        <Match pattern="/ns/:namespace/file" component={FileBrowser} />
        <Miss component={Page404} />
      </div>
    );
  }
}

Home.propTypes = {
  params: PropTypes.shape({
    namespace : PropTypes.string
  })
};
