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

/*
  Usage:
    <Card
      header={<div>Header</div>}
      title='some title'
      body={<div>Body</div>} | 'some string'
      footer={<div>Footer</div>}
      cardClass='custom-class-name'
      size='SM | MD | LG'
      cardStyle={{width: '50%';}}
      closeable
      onClose={this.onCloseFunction.bind(this)}
    >
      <div>
        the content in here will overwrite the body prop
      </div>
    </Card>
*/

import PropTypes from 'prop-types';
import React, { Component } from 'react';
require('./Card.scss');

var classNames = require('classnames');

export default class Card extends Component {
  getHeader() {
    let closeButton;
    if (this.props.closeable) {
      closeButton = <span className="fa fa-times" onClick={this.props.onClose} />;
    }

    const titleHeader = <h3 className="card-title">{this.props.title}</h3>;
    const headerContent = this.props.title ? titleHeader : this.props.header;

    const headerElem = (
      <div className="card-header">
        {headerContent}
        {closeButton}
      </div>
    );

    return this.props.header || this.props.title ? headerElem : null;
  }

  getBody() {
    const content = this.props.children ? this.props.children : this.props.body;

    const bodyElem = <div className="card-body">{content}</div>;

    return content ? bodyElem : null;
  }

  getFooter() {
    const footerElem = <div className="card-footer">{this.props.footer}</div>;

    return this.props.footer ? footerElem : null;
  }

  render() {
    const cardClass = classNames(
      'cask-card',
      this.props.cardClass,
      { [`card-${this.props.size}`]: this.props.size },
      { 'card-LG': !this.props.size }
    );

    return (
      <div className={cardClass} style={this.props.cardStyle}>
        {this.getHeader()}
        {this.getBody()}
        {this.getFooter()}
      </div>
    );
  }
}

Card.propTypes = {
  header: PropTypes.element,
  body: PropTypes.oneOfType([PropTypes.string, PropTypes.element]),
  footer: PropTypes.element,
  title: PropTypes.string,
  children: PropTypes.node,
  closeable: PropTypes.bool,
  onClose: PropTypes.func,
  cardClass: PropTypes.string,
  size: PropTypes.oneOf(['SM', 'MD', 'LG']),
  cardStyle: PropTypes.object,
};
