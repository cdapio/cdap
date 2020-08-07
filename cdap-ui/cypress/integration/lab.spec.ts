import {loginIfRequired} from '../helpers';
import {dataCy} from '../helpers';

let headers = {};

describe('Lab ', () => {
  // Uses API call to login instead of logging in manually through UI
  before(() => {
    loginIfRequired().then(() => {
      cy.getCookie('CDAP_Auth_Token').then((cookie) => {
        if (!cookie) {
          return;
        }
        headers = {
          Authorization: 'Bearer ' + cookie.value,
        };
      });
    });
  });

  it('should have cdap-common-experiment disabled by default', () => {
    cy.visit('/cdap/lab');
    cy.get(`${dataCy('cdap-common-experiment-switch')} input`).should('have.value', 'false');
  });

  describe(' toggle experiment wrapper ', () => {
    it('should show default component when cdap-common-experiment is disabled', () => {
      cy.visit('/cdap/lab-experiment-test');
      cy.get(dataCy('default-feature-toggle-selector')).should('have.text', 'This is default component for the toggle.');
    });

    it('show experimental component when cdap-common-experiment is enabled', () => {
      cy.visit('/cdap/lab');
      cy.get(`${dataCy('cdap-common-experiment-switch')} input`).should('have.value', 'false');
      cy.get(`${dataCy('cdap-common-experiment-switch')}`).click();
      cy.get(`${dataCy('cdap-common-experiment-switch')} input`).should('have.value', 'true');

      cy.visit('/cdap/lab-experiment-test');
      cy.get(dataCy('experimental-feature-toggle-selector')).should('have.text', 'This is experimental component for the toggle.');

      cy.visit('/cdap/lab');
      cy.get(`${dataCy('cdap-common-experiment-switch')}`).click();
    });
  });

  describe(' experiment wrapper ', () => {
    it('should not show experimental component when cdap-common-experiment is disabled', () => {
      cy.visit('/cdap/lab-experiment-test');
      cy.get(dataCy('experimental-feature-selector')).should('not.exist');
    });

    it('should show experimental component when cdap-common-experiment is enabled', () => {
      cy.visit('/cdap/lab');
      cy.get(`${dataCy('cdap-common-experiment-switch')} input`).should('have.value', 'false');
      cy.get(`${dataCy('cdap-common-experiment-switch')}`).click();
      cy.get(`${dataCy('cdap-common-experiment-switch')} input`).should('have.value', 'true');

      cy.visit('/cdap/lab-experiment-test');
      cy.get(dataCy('experimental-feature-selector')).should('have.text','This is an experimental component.');
    });
  });


});
