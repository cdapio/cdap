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
import {mount} from 'enzyme';
import ProductDropdown from 'components/Header/ProductDropdown';
import '../../../../../ui-utils/url-generator.js';
import NamespaceStore from 'services/NamespaceStore';
import NamespaceActions from 'services/NamespaceStore/NamespaceActions';

window.CDAP_CONFIG = {
  securityEnabled: false
};
let productdropdown;
describe('ProductDropdown Unit tests', () => {
  beforeEach(() => {
    productdropdown = mount(
      <ProductDropdown nativeLink={true}/>
    );
    NamespaceStore.dispatch({
      type: NamespaceActions.selectNamespace,
      payload: {
        selectedNamespace: 'default'
      }
    });
  });

  it('Should render', () => {
    expect(productdropdown.find('.product-dropdown').length).toBe(1);
    expect(productdropdown.state('currentNamespace')).toBe('default');
  });

  it('Should render OLD CDAP link properly', () => {
    let oldCDAPUrl = `/oldcdap/ns/default`;
    expect(productdropdown.find({href: oldCDAPUrl}).node.href).toBe(oldCDAPUrl);
  });

  it('Should render OLD CDAP link on namespace change', () => {
    let oldCDAPUrl = `/oldcdap/ns/NS1`;
    NamespaceStore.dispatch({
      type: NamespaceActions.selectNamespace,
      payload: {
        selectedNamespace: 'NS1'
      }
    });
    expect(productdropdown.find({href: oldCDAPUrl}).node.href).toBe(oldCDAPUrl);
  });

});
