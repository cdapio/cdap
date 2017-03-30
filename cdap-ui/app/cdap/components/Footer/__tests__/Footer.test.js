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
import React from 'react';
import { mount } from 'enzyme';
import Footer from 'components/Footer';
describe('Footer Unit tests - ', () => {
  it('Should Render', () => {
    const footer = mount(
      <Footer
        copyrightYear="2016"
        version="4.1.1-SNAPSHOT"
      />
    );
    expect(footer.instance().version).toBe('4.1.1-SNAPSHOT');
  });
  it('Should handle invalid cases with default values', () => {
    const footer = mount(
      <Footer />
    );
    expect(footer.instance().version).toBe('--unknown--');
    expect(footer.instance().copyrightYear).toBe(new Date().getFullYear());
  });
});
