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
import {objectQuery} from 'services/helpers';
import SchemaStore from 'components/SchemaEditor/SchemaStore';
import SchemaEditor from 'components/SchemaEditor';
import {getParsedSchema} from 'components/SchemaEditor/SchemaHelpers';
import T from 'i18n-react';

require('./SchemaTab.scss');

export default class SchemaTab extends Component {
  constructor(props) {
    super(props);
    this.state = {
      entity: this.props.entity
    };
  }

  componentWillMount() {
    if (!this.props.entity.schema) { return; }

    let fields = getParsedSchema(this.props.entity.schema);
    this.setSchema({fields});
  }

  componentWillReceiveProps(nextProps) {
    let entitiesMatch = objectQuery(nextProps, 'entity', 'name') === objectQuery(this.props, 'entity', 'name');
    if (!entitiesMatch) {
      this.setState({
        entity: nextProps.entity
      });

      let fields = getParsedSchema(nextProps.entity.schema);
      this.setSchema({fields});
    }
  }

  componentWillUnmount() {
    SchemaStore.dispatch({
      type: 'RESET'
    });
  }

  setSchema(schema) {
    SchemaStore.dispatch({
      type: 'FIELD_UPDATE',
      payload: {
        schema
      }
    });
  }

  render() {
    return (
      <div className="schema-tab">
        <fieldset
          className="disable-schema"
          disabled
        >
          {
            this.state.entity.schema ?
              <SchemaEditor />
            :
            <div>
              <i>{T.translate('features.Overview.SchemaTab.emptyMessage')}</i>
            </div>
          }
        </fieldset>
      </div>
    );
  }
}

SchemaTab.propTypes = {
  entity: PropTypes.shape({
    schema: PropTypes.oneOfType([PropTypes.string, PropTypes.object])
  })
};
