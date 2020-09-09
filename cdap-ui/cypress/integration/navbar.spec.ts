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

import { Theme } from '../../app/cdap/services/ThemeHelper';
import * as Helpers from '../helpers';

const NAVBAR_MENU_HIGHLIGHT_COLOR = 'rgb(220, 224, 234)';
const NAVBAR_MENU_FONT_COLOR = 'rgb(0, 118, 220)';
const NAVBAR_BG_COLOR = 'rgb(51, 51, 51)';
const NAVBAR_BG_COLOR_LIGHT = 'rgb(59, 120, 231)';
let headers = {};
describe('Navbar tests', () => {
  before(() => {
    Helpers.loginIfRequired()
      .then(() => {
        cy.getCookie('CDAP_Auth_Token').then((cookie) => {
          if (!cookie) {
            return Helpers.getSessionToken({});
          }
          headers = {
            Authorization: 'Bearer ' + cookie.value,
          };
          return Helpers.getSessionToken(headers);
        });
      })
      .then(
        (sessionToken) => (headers = Object.assign({}, headers, { 'Session-Token': sessionToken }))
      );
  });
  after(() => {
    cy.request({
      url: '/updateTheme',
      method: 'POST',
      body: {
        uiThemePath: 'config/themes/default.json',
      },
      headers,
    });
  });
  it('Should have right bgcolor for default theme', () => {
    cy.visit('/cdap');
    cy.get('[data-cy="app-navbar"]').then((navbar) => {
      const bgcolor = navbar.css('background-color');
      expect(bgcolor).to.be.eq(NAVBAR_BG_COLOR);
    });
  });
  it('Should have right features enabled', () => {
    cy.contains(Theme.featureNames.dashboard);
    cy.contains(Theme.featureNames.hub);
    cy.get('[data-cy="navbar-hamburger-icon"]').click();
    cy.contains(Theme.featureNames.pipelines);
    cy.contains(Theme.featureNames.dataPrep);
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
        uiThemePath: 'config/themes/light.json',
      },
      headers,
    }).then(() => {
      cy.visit('/cdap');
      cy.get('[data-cy="app-navbar"]').then((navbar) => {
        const bgcolor = navbar.css('background-color');
        expect(bgcolor).to.be.eq(NAVBAR_BG_COLOR_LIGHT);
      });
    });
  });
  it('Should have right features enabled/disabled in light theme', () => {
    cy.get(`[data-cy="${Theme.featureNames.dashboard}`).should('not.exist');
    cy.contains(Theme.featureNames.hub);
    cy.get('[data-cy="navbar-hamburger-icon"]').click();
    cy.contains(Theme.featureNames.pipelines);
    cy.contains(Theme.featureNames.dataPrep);
    cy.contains(Theme.featureNames.metadata);
    cy.contains(Theme.featureNames.pipelineStudio);
    cy.get('[data-cy="navbar-hamburger-icon"]').click();
  });
  it('Should have the right features highlighted in the drawer', () => {
    const assetFeatureHighlight = (featureSelector) => {
      cy.get('[data-cy="navbar-hamburger-icon"]').click();
      cy.get(`[data-cy="${featureSelector}"]`).click();
      cy.get('[data-cy="navbar-hamburger-icon"]').click();
      cy.get(`[data-cy="${featureSelector}"]`).then((subject) => {
        subject.css('background-color', NAVBAR_MENU_HIGHLIGHT_COLOR);
        subject.css('color', NAVBAR_MENU_FONT_COLOR);
      });
      cy.get('[data-cy="navbar-hamburger-icon"]').click();
    };

    cy.visit('/cdap');
    cy.get('[data-cy="navbar-hamburger-icon"]').click();
    cy.get(`[data-cy="navbar-control-center-link"]`).then((subject) => {
      subject.css('background-color', NAVBAR_MENU_HIGHLIGHT_COLOR);
      subject.css('color', NAVBAR_MENU_FONT_COLOR);
    });
    cy.get('[data-cy="navbar-hamburger-icon"]').click();

    assetFeatureHighlight('navbar-pipelines-link');
    assetFeatureHighlight('navbar-pipeline-studio-link');
    assetFeatureHighlight('navbar-metadata-link');
    assetFeatureHighlight('navbar-project-admin-link');
  });
  it('Should close when hub is opened', () => {
    cy.visit('/cdap');
    cy.get('[data-cy="navbar-hamburger-icon"]').click();
    cy.get('#navbar-hub').click();
    // For the animation -_-
    cy.wait(200).then(() => {
      cy.get('[data-cy="navbar-drawer"]').then((drawerEl) => {
        const visibility = drawerEl.css('visibility');
        expect(visibility).to.be.eq('hidden');
      });
    });
  });
});
