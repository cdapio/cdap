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

import React, { Component, PropTypes } from 'react';
import OverviewMetaSection from 'components/Overview/OverviewMetaSection';
import OverviewHeader from 'components/Overview/OverviewHeader';
import ExploreTablesStore from 'services/ExploreTables/ExploreTablesStore';
import {fetchTables} from 'services/ExploreTables/ActionCreator';
import {objectQuery} from 'services/helpers';
import NamespaceStore from 'services/NamespaceStore';
import {MyDatasetApi} from 'api/dataset';
import {MyMetadataApi} from 'api/metadata';
import isNil from 'lodash/isNil';
import isEmpty from 'lodash/isEmpty';
import shortid from 'shortid';
import T from 'i18n-react';
import DatasetDetaildViewTab from 'components/DatasetDetailedView/Tabs';
import {MySearchApi} from 'api/search';
import {parseMetadata} from 'services/metadata-parser';

export default class DatasetDetailedView extends Component {
  constructor(props) {
    super(props);

    this.state = {
      entityDetail: objectQuery(this.props, 'location', 'state', 'entityDetail') | {
        schema: null,
        programs: []
      },
      loading: true,
      entityMetadata: objectQuery(this.props, 'location', 'state', 'entityMetadata') || {},
      isInvalid: false
    };
  }

  componentWillMount() {
    let {namespace, datasetId} = this.props.params;
    if (!namespace) {
      namespace = NamespaceStore.getState().selectedNamespace;
    }
    ExploreTablesStore.dispatch(
      fetchTables(namespace)
    );

    if (!this.state.entityDetail.schema || this.state.entityDetail.programs.length === 0) {
      const datasetParams = {
        namespace,
        datasetId
      };

      const metadataParams = {
        namespace,
        entityType: 'datasets',
        entityId: datasetId,
        scope: 'SYSTEM'
      };

      MyMetadataApi.getProperties(metadataParams)
        .combineLatest(MyDatasetApi.getPrograms(datasetParams))
        .subscribe((res) => {
          let appId;
          let programs = res[1].map((program) => {
            program.uniqueId = shortid.generate();
            appId = program.application.applicationId;
            program.app = appId;
            program.name = program.id;
            return program;
          });

          let entityDetail = {
            programs,
            schema: res[0].schema,
            name: appId, // FIXME: Finalize on entity detail for fast action
            app: appId,
            id: datasetId,
            type: 'dataset'
          };

          this.setState({
            entityDetail
          }, () => {
            setTimeout(() => {
              this.setState({
                loading: false
              });
            }, 1000);
          });
        });
    }

    if (
      isNil(this.state.entityMetadata) ||
      isEmpty(this.state.entityMetadata)
    ) {
      // FIXME: This is NOT the right way. Need to figure out a way to be more efficient and correct.

      MySearchApi
        .search({
          namespace,
          query: this.props.params.datasetId
        })
        .map(res => res.results.map(parseMetadata))
        .subscribe(entityMetadata => {
          this.setState({
            entityMetadata: entityMetadata[0],
            loading: false
          });
        });
    }

    if (
      isNil(this.state.entityMetadata) ||
      isEmpty(this.state.entityMetadata) ||
      isNil(this.state.entity) ||
      isEmpty(this.state.entity)
    ) {
      this.setState({
        loading: true
      });
    }

  }

  render() {
    if (this.state.loading) {
      return (
        <div className="app-detailed-view">
          <div className="fa fa-spinner fa-spin fa-3x"></div>
        </div>
      );
    }

    const title = T.translate('commons.entity.dataset.singular');

    return (
      <div className="app-detailed-view">
        <OverviewHeader
          icon="icon-datasets"
          title={title}
        />
        <OverviewMetaSection entity={this.state.entityMetadata} />
        <DatasetDetaildViewTab
          params={this.props.params}
          pathname={this.props.location.pathname}
          entity={this.state.entityDetail}
        />
      </div>
    );
  }
}

DatasetDetailedView.propTypes = {
  params: PropTypes.shape({
    datasetId: PropTypes.string,
    namespace: PropTypes.string
  }),
  location: PropTypes.any
};
