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
var classNames = require('classnames');
require('./Card.less');
export default class Card extends Component {
  constructor(props) {
    super(props);
    this.props = props;
  }
  render() {
    var statusClasses = classNames('card-status',
      { 'error' : this.props.error },
      { 'success': this.props.success },
      { 'hide': !this.props.error && !this.props.success }
    );
    return (
      <Card-Wrapper>
        <div className="card">
          <div className="card-head">
            <h3 className="card-title">
              {this.props.title}
            </h3>
            {/* For now just closing. No context is being passed out*/}
            <span
              className={!this.props.closeable ? 'hide': 'fa fa-times'}
              onClick={this.props.onClose}
            >
            </span>
          </div>
          <div className="card-body">
            {this.props.content ? this.props.content : this.props.children}
          </div>
          <div className={statusClasses}>
            {this.props.error ? this.props.error : this.props.success}
          </div>
        </div>
      </Card-Wrapper>
    );
  }
}

Card.propTypes = {
  title: PropTypes.string,
  content: PropTypes.string,
  children: PropTypes.node,
  closeable: PropTypes.boolean,
  success: PropTypes.string,
  error: PropTypes.string,
  onClose: PropTypes.function
};
