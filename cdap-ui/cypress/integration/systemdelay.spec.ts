import { loginIfRequired } from '../helpers';
import { dataCy } from '../helpers';
import { getExperimentValue, isExperimentEnabled } from '../../app/cdap/services/helpers';

let headers = {};
const EXPERIMENT_ID='system-delay-notification';

describe('System delay notification ', () => {
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

    beforeEach(() => {
        cy.visit('/cdap/ns/default/lab');
        cy.get(`${dataCy(`${EXPERIMENT_ID}-switch`)} input`).should('have.value', 'true');
        // Makes max allowed delay for requests to be 0 i.e all requests would be considered slow.
        cy.get(dataCy(`${EXPERIMENT_ID}-field`)).type('0');
    });

    it('should be visible when there is a delay', () => {
        cy.visit('/cdap/ns/default');
        expect(getExperimentValue(EXPERIMENT_ID)).to.be.eq('0');
        expect(isExperimentEnabled(EXPERIMENT_ID)).to.be.true;
        cy.get(dataCy('system-delay-snackbar')).should('be.visible');
        cy.visit('/pipelines/ns/default/studio');
        cy.get(dataCy('system-delay-snackbar')).should('be.visible');
        // Waiting to check that the notification is persistent
        cy.wait(13000);
        cy.get(dataCy('system-delay-snackbar')).should('be.visible');
    });

    it('should not be visible when there is no delay', () => {
        // Removes the '0' set for max delay of requests
        expect(getExperimentValue(EXPERIMENT_ID)).to.be.eq('0');
        cy.visit('/cdap/ns/default');
        window.localStorage.removeItem(`${EXPERIMENT_ID}-value`);
        window.localStorage.removeItem(EXPERIMENT_ID);
        expect(getExperimentValue(`${EXPERIMENT_ID}-value`)).to.be.eq(null);
        expect(window.localStorage.getItem('system-delay-notification')).to.be.eq(null);
        cy.get(dataCy('system-delay-snackbar')).should('not.be.visible');
        // Waiting to check that the snackbar does not appear on the next health check
        cy.wait(13000);
        cy.get(dataCy('system-delay-snackbar')).should('not.be.visible');
    });
    
    it('should not be visible when user asks to not see again', () => {
        cy.visit('/cdap/ns/default');
        cy.get(dataCy('navbar-hamburger-icon')).should('be.visible');
        expect(getExperimentValue(EXPERIMENT_ID)).to.be.eq('0');
        expect(isExperimentEnabled(EXPERIMENT_ID)).to.be.true;
        cy.get(dataCy('system-delay-snackbar')).should('be.visible');
        cy.get(dataCy('do-not-show-delay-btn')).should('be.visible');
        cy.get(dataCy('do-not-show-delay-btn')).click({ force: true });
        cy.get(dataCy('system-delay-snackbar')).should('not.be.visible');
        cy.then(() => {
            expect(getExperimentValue(EXPERIMENT_ID)).to.be.eq(null);
            expect(isExperimentEnabled(EXPERIMENT_ID)).to.be.false;
        });
        cy.visit('/cdap/ns/default/lab');
        cy.get(`${dataCy(`${EXPERIMENT_ID}-switch`)} input`).should('have.value', 'false');
    });
});
