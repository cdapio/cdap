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

import React, { Component } from 'react';
import PropTypes from 'prop-types';
import {MyAppApi} from 'api/app';
import NamespaceStore from 'services/NamespaceStore';

require('./PipelineTags.scss');

export default class PipelineTags extends Component {
  static propTypes = {
    pipelineName: PropTypes.string
  };

  constructor(props) {
    super(props);
  }

  componentWillMount() {
    let namespace = NamespaceStore.getState().selectedNamespace;

    let params = {
      namespace,
      appId: this.props.pipelineName
    };

    MyAppApi.getMetadata(params)
      .subscribe((res) => {
        let systemTags = [],
            userTags = [];

        res.forEach((metadata) => {
          if (metadata.scope === 'USER') {
            userTags = metadata.tags;
          } else if (metadata.scope === 'SYSTEM') {
            systemTags = metadata.tags;
          }
        });

        this.setState({
          systemTags,
          userTags
        });
      });
  }

  state = {
    systemTags: [],
    userTags: []
  };

  renderTag(tag, scope) {
    return (
      <div
        className="pipeline-tag"
        key={tag}
      >
        <span className={`tag-content ${scope}`}>
          {tag}
        </span>
      </div>
    );
  }

  render() {
    return (
      <div className="pipeline-tags-container">
        {
          this.state.userTags.map((tag) => {
            return this.renderTag(tag, 'user');
          })
        }
        {
          this.state.systemTags.map((tag) => {
            return this.renderTag(tag, 'system');
          })
        }
      </div>
    );
  }
}
