/*
 * Copyright © 2018 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the 'License'); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an 'AS IS' BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
*/

import { Theme } from 'services/ThemeHelper';

describe('Navbar tests', () => {
  after(() => {
    cy.request({
      url: '/updateTheme',
      method: 'POST',
      body: {
        uiThemePath: 'cdap-ui/server/config/themes/default.json',
      },
    });
  });
  it('Should have right bgcolor for default theme', () => {
    cy.visit('/cdap');
    cy.get('[data-cy="app-navbar"]').then((navbar) => {
      const bgcolor = navbar.css('background-color');
      expect(bgcolor).to.be.eq('rgb(51, 51, 51)');
    });
  });
  it('Should have right features enabled', () => {
    cy.contains(Theme.featureNames.dashboard);
    cy.contains(Theme.featureNames.hub);
    cy.get('[data-cy="navbar-hamburger-icon"]').click();
    cy.contains(Theme.featureNames.pipelines);
    cy.contains(Theme.featureNames.dataPrep);
    cy.contains(Theme.featureNames.analytics);
    cy.contains(Theme.featureNames.rulesEngine);
    cy.contains(Theme.featureNames.metadata);
  });
  it('Should have the drawer invisible by default', () => {
    cy.get('[data-cy="navbar-hamburger-icon"]').click();
    // For the animation -_-
    cy.wait(200).then(() => {
      cy.get('[data-cy="navbar-drawer"]').then((drawerEl) => {
        const visibility = drawerEl.css('visibility');
        expect(visibility).to.be.eq('hidden');
      });
    });
  });
  it('Should have right bgcolor for light theme', () => {
    cy.request({
      url: '/updateTheme',
      method: 'POST',
      body: {
        uiThemePath: 'cdap-ui/server/config/themes/light.json',
      },
    }).then(() => {
      cy.visit('/cdap');
      cy.get('[data-cy="app-navbar"]').then((navbar) => {
        const bgcolor = navbar.css('background-color');
        expect(bgcolor).to.be.eq('rgb(59, 120, 231)');
      });
    });
  });
  it('Should have right features enabled/disabled in light theme', () => {
    cy.contains(Theme.featureNames.dashboard).should('not.exist');
    cy.contains(Theme.featureNames.hub);
    cy.get('[data-cy="navbar-hamburger-icon"]').click();
    cy.contains(Theme.featureNames.pipelines);
    cy.contains(Theme.featureNames.dataPrep);
    cy.contains(Theme.featureNames.analytics);
    cy.contains(Theme.featureNames.rulesEngine);
    cy.contains(Theme.featureNames.metadata);
    cy.get('[data-cy="navbar-hamburger-icon"]').click();
  });
});
