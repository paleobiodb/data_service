// Shared fixtures for the browser tests of the PBDB web applications.
//
// Added by Claude under the guidance of Michael McClennen.
//
// The pages are served by Classic, which decides from its own login whether the viewer is
// logged in and a contributor. The tests do not log in to Classic. Instead, the 'appPage'
// fixture rewrites those two settings in the page as it arrives, and gives the browser the
// session cookie of a test session. The page is opened with 'debug=api', which sends its
// API requests to /dtest1.2/, the test API server, where that session exists. Any request
// that would change data through the real API (/data1.2/) is blocked and fails the test.
//
// Some pages require a login to Classic, which redirects a guest to the login page. For
// these, the browser is given the cookie 'pbdb_webapp_test' with the value of the setting
// WEBAPP_TEST_COOKIE in Classic's pbdb.conf, which lets the page load without a login. That
// setting is made only on a development machine.

const base = require('@playwright/test');
const { execFileSync } = require('child_process');
const fs = require('fs');

const CONTAINER = process.env.PBAPI_CONTAINER || 'paleobiodb-pbapi-1';
const PBAPI_DIR = process.env.PBAPI_DIR || '/var/paleomacro/pbapi';

const CLASSIC_CONF = process.env.PBDB_CLASSIC_CONF || '/var/paleobiodb/frontend/classic/pbdb.conf';

const WRITE_OPS = /\/(add|update|addupdate|delete|set|checknames)\b[^/]*$/;


// Return the value of WEBAPP_TEST_COOKIE from Classic's configuration, or null.

function webappTestCookie () {

	try {
		const conf = fs.readFileSync(CLASSIC_CONF, 'utf8');
		const m = /^\s*WEBAPP_TEST_COOKIE\s*=\s*(\S+)/m.exec(conf);
		return m ? m[1] : null;
	}

	catch (e) {
		return null;
	}
}


// Run a command of e2e/testdb.pl in the pbapi container, and return its result.

function testdb (...args) {

	const output = execFileSync('docker', ['exec', '-w', PBAPI_DIR, CONTAINER,
										   'perl', 'e2e/testdb.pl', ...args],
								{ encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] });

	const line = output.split('\n').find(l => l.startsWith('RESULT '));

	if ( ! line ) throw new Error(`testdb ${args.join(' ')}: no result`);

	return JSON.parse(line.substring(7));
}


// Return the number from an identifier such as 'col:12'.

function idNo (oid) {
	const m = /(\d+)$/.exec(oid || '');
	return m ? m[1] : null;
}


const test = base.test.extend({

	// The session the page logs in with, in the test session table.

	session: [ 'SESSION-ENTERER', { option: true } ],

	// A page with the login settings rewritten. 'apiRequests' lists every request made to
	// the API, 'dialogs' every dialog the page showed, which are accepted, and 'alerts()'
	// returns the alerts it showed.

	appPage: async ({ page, context, session, baseURL }, use) => {

		const host = new URL(baseURL).hostname;

		await context.addCookies([ { name: 'session_id', value: session, domain: host,
									 path: '/' } ]);

		const testCookie = webappTestCookie();

		if ( testCookie ) {
			await context.addCookies([ { name: 'pbdb_webapp_test', value: testCookie,
										 domain: host, path: '/' } ]);
		}

		await page.route(/\/app\/[\w-]+(\/[\w.-]+)?(\?[^#]*)?$/, async route => {
			if ( route.request().resourceType() != 'document' ) return route.continue();
			const response = await route.fetch();
			let body = await response.text();
			body = body.replace(/\bis_logged_in = false;/, 'is_logged_in = true;')
				.replace(/\bis_contributor = false;/, 'is_contributor = true;');
			await route.fulfill({ response, body });
		});

		const blocked = [];

		await page.route(/\/data1\.2\//, route => {
			const url = new URL(route.request().url());
			if ( WRITE_OPS.test(url.pathname) || route.request().method() != 'GET' ) {
				blocked.push(route.request().method() + ' ' + url.pathname);
				return route.abort();
			}
			return route.continue();
		});

		const apiRequests = [];

		page.on('request', r => {
			if ( /\/(data|dtest)1\.2\//.test(r.url()) ) apiRequests.push(r);
		});

		const dialogs = [];

		page.on('dialog', d => { dialogs.push(d.message()); d.accept(); });

		const errors = [];

		page.on('pageerror', e => errors.push(e.message));

		page.apiRequests = apiRequests;
		page.dialogs = dialogs;
		page.pageErrors = errors;

		// The pages served by Classic include wing.js, which replaces window.alert with a
		// PNotify notice. An alert therefore does not appear in 'dialogs', and this returns
		// the text of every notice shown instead. A confirmation is still a dialog.

		page.alerts = () => page.locator('.ui-pnotify-text').allInnerTexts();

		await use(page);

		base.expect(blocked, 'requests that would change the real database').toEqual([]);
	},
});


module.exports = { test, expect: base.expect, testdb, idNo };
