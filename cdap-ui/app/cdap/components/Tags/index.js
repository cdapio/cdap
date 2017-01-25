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
import {objectQuery} from 'services/helpers';
require('./Tags.scss');
import T from 'i18n-react';

export default class Tags extends Component {
  constructor(props) {
    super(props);
    this.state = {
      tags: {
        system: objectQuery(this.props, 'entity', 'metadata', 'metadata', 'SYSTEM', 'tags') || [],
        user: objectQuery(this.props, 'entity', 'metadata', 'metadata', 'USER', 'tags') || []
      }
    };
  }
  componentWillReceiveProps(nextProps) {
    let {entity} = nextProps;
    if (entity.id !== this.props.entity.id) {
      this.setState({
        tags: {
          system: objectQuery(nextProps, 'entity', 'metadata', 'metadata', 'SYSTEM', 'tags') || [],
          user: objectQuery(this.props, 'entity', 'metadata', 'metadata', 'USER', 'tags') || []
        }
      });
    }
  }
  render() {
    let tagsCount = this.state.tags.system.length + this.state.tags.user.length;
    return (
      <div className="tags-holder">
        <strong> {T.translate('features.Tags.label')}({tagsCount}): </strong>
        {
          !tagsCount ?
            <i>{T.translate('features.Tags.notags')}</i>
          :
            null
        }
        <span>
          {
            this.state.tags.system.map(tag => {
              return (
                <span className="btn btn-secondary">
                  <span>{tag}</span>
                </span>
              );
            })
          }
          {
            this.state.tags.user.map(tag => {
              return (
                <span className="btn btn-secondary">
                  <span>{tag}</span>
                  <span className="fa fa-times"></span>
                </span>
              );
            })
          }
        </span>
      </div>
    );
  }
}
Tags.propTypes = {
  entity: PropTypes.object
};
