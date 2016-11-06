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
import T from 'i18n-react';
import classnames from 'classnames';
require('./ProductsDropdown.less');
import head from 'lodash/head';

export default class ProductsDrawer extends Component {
  constructor(props) {
    super(props);
    let products = [
      {
        link: '/cask-cdap',
        label: T.translate('commons.cdap'),
        name: 'cdap',
        icon: 'icon-fist'
      },
      {
        link: '/cask-hydrator',
        label: T.translate('commons.hydrator'),
        name: 'hydrator',
        icon: 'icon-hydrator'
      },
      {
        link: '/cask-tracker',
        label: T.translate('commons.tracker'),
        name: 'tracker',
        icon: 'icon-tracker'
      }
    ];
    let currentChoice = head(products.filter(product => product.name === props.currentChoice));
    this.state = {
      productsDropdown: false,
      currentChoice,
      products
    };
  }
  toggle() {
    this.setState({
      productsDropdown: !this.state.productsDropdown
    });
  }
  render() {
    return (
      <div>
        <div className="brand-header"
             onClick={this.toggle.bind(this)}>
          <div className="navbar-brand">
            <div className={classnames("brand-icon text-center", this.state.currentChoice.name)}>
              <span className={classnames("fa", this.state.currentChoice.icon)}></span>
            </div>
          </div>
          <div className="menu-item product-title">
            <span>{this.state.currentChoice.label}</span>
          </div>
        </div>

        {
          this.state.productsDropdown ?
            (
              <div className="display-container"
                   onClick={this.toggle.bind(this)}>
                <div className="sidebar pull-right">
                  <a href="/cask-cdap"
                     className="brand sidebar-item top">
                    <div className="brand-icon text-center cdap">
                      <span className="icon-fist"></span>
                    </div>
                    <div className="product-name">
                      <span>{T.translate('commons.cdap')}</span>
                    </div>
                  </a>
                  <h5> Extensions: </h5>
                  <a href="/cask-hydrator"
                     className="brand sidebar-item">
                      <div className="brand-icon text-center hydrator">
                        <span className="icon-hydrator"></span>
                      </div>
                      <div className="product-name">
                        <span>{T.translate('commons.hydrator')}</span>
                      </div>
                   </a>
                   <a href="/cask-tracker"
                      className="brand sidebar-item">
                       <div className="brand-icon text-center tracker">
                         <span className="icon-tracker"></span>
                       </div>
                       <div className="product-name">
                         <span>{T.translate('commons.tracker')}</span>
                       </div>
                    </a>
                </div>
              </div>
            )
          :
            null
        }
      </div>
    );
  }
}

ProductsDrawer.propTypes = {
  currentChoice: PropTypes.string
};
