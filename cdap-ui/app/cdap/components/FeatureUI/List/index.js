
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

import React from 'react';
import isEmpty from 'lodash/isEmpty';
import PropTypes from 'prop-types';

require("./List.scss");

class List extends React.Component {
  constructor(props) {
    super(props);
  }

  render() {
    let listData = isEmpty(this.props.dataProvider) ? [] : this.props.dataProvider;
    return (
      <div>
        {
          this.props.header &&
          <div className={this.props.headerClass} key={this.props.header}
            onClick={this.props.onHeaderClick}>{this.props.header}
          </div>
        }
        {
          listData.map(item => {
            return (
              <div className="schema-list-item" key={item.parent+item.child}>
                <div className="parent-item">{item.parent+ ": "} </div>
                <div className="child-item">{item.child}</div>
              </div>);
          })
        }
      </div>);
  }
}
export default List;
List.propTypes = {
  dataProvider: PropTypes.array,
  header: PropTypes.string,
  headerClass: PropTypes.string,
  onHeaderClick: PropTypes.func
};
