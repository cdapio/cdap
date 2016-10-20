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
import {MyAppApi} from 'api/app';
import {MyArtifactApi} from 'api/artifact';
import {MyDatasetApi} from 'api/dataset';
import {MyStreamApi} from 'api/stream';
import NamespaceStore from 'services/NamespaceStore';
import T from 'i18n-react';

export default class FastActions extends Component {
  constructor(props) {
    super(props);
  }

  setupApiAndParams() {
    let api;
    let params = {
      namespace: NamespaceStore.getState().selectedNamespace
    };

    switch (this.props.entity.type) {
      case 'artifact':
        api = MyArtifactApi;
        params.artifactId = this.props.entity.id;
        params.version = this.props.entity.version;
        break;
      case 'application':
        api = MyAppApi;
        params.appId = this.props.entity.id;
        break;
      case 'stream':
        api = MyStreamApi;
        params.streamId = this.props.entity.id;
        break;
      case 'datasetinstance':
        api = MyDatasetApi;
        params.datasetId = this.props.entity.id;
        break;
    }

    return { api, params };
  }

  handleEntityDelete() {
    let text = T.translate(
      'features.Home.FastActions.deleteConfirmation',
      { entityId: this.props.entity.id}
    );
    let confirmation = confirm(text);

    if (!confirmation) { return; }

    let { api, params } = this.setupApiAndParams();

    api.delete(params)
      .subscribe(
        () => {
          this.props.onUpdate();
        },
        (err) => {
          alert(err);
        }
      );
  }

  handleEntityTruncate() {
    let text = T.translate(
      'features.Home.FastActions.truncateConfirmation',
      { entityId: this.props.entity.id}
    );
    let confirmation = confirm(text);
    if (!confirmation) { return; }

    let { api, params } = this.setupApiAndParams();

    api.truncate(params)
      .subscribe(
        () => {
          this.props.onUpdate();
        },
        (err) => {
          alert(err);
        }
      );
  }

  handleRenderDelete(disabled) {
    return (
      <button
        className="btn btn-link"
        onClick={this.handleEntityDelete.bind(this)}
        disabled={disabled}
      >
        <span className="fa fa-trash"></span>
      </button>
    );
  }

  handleRenderTruncate() {
    return (
      <button
        className="btn btn-link"
        onClick={this.handleEntityTruncate.bind(this)}
      >
        <span className="fa fa-scissors"></span>
      </button>
    );
  }

  handleRenderNavigate() {
    let link = window.getAbsUIUrl({
      uiApp: 'cask-hydrator',
      namespaceId: NamespaceStore.getState().selectedNamespace,
      entityType: 'view',
      entityId: this.props.entity.id
    });

    return (
      <a href={link} className="btn btn-link">
        <span className="fa fa-external-link-square"></span>
      </a>
    );
  }

  createApplicationActions() {
    const tags = this.props.entity.metadata.metadata.SYSTEM.tags;

    let navigateAction;
    if (tags.includes('cdap-data-pipeline') || tags.includes('cdap-data-streams')) {
      navigateAction = this.handleRenderNavigate();
    }

    return (
      <span>
        {navigateAction}
        {this.handleRenderDelete()}
      </span>
    );
  }

  createArtifactActions() {
    return (
      <span>
        {this.handleRenderDelete(this.props.entity.scope === 'SYSTEM')}
      </span>
    );
  }

  createDatasetAndStreamsActions() {
    return (
      <span>
        {this.handleRenderTruncate()}
        {this.handleRenderDelete()}
      </span>
    );
  }


  handleEntityFastActions() {
    switch (this.props.entity.type) {
      case 'application':
        return this.createApplicationActions();
      case 'artifact':
        return this.createArtifactActions();
      case 'datasetinstance':
      case 'stream':
        return this.createDatasetAndStreamsActions();
    }
  }


  render () {
    return (
      <h4 className="text-center">
        {this.handleEntityFastActions()}
      </h4>
    );
  }
}

FastActions.propTypes = {
  entity: PropTypes.object,
  onUpdate: PropTypes.func
};
