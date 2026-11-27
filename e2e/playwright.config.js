// Playwright configuration for the browser tests of the PBDB web applications. The pages
// are served by Classic through the local nginx, under the name paleobiodb.local, and their
// API requests go to the test API server on port 3999 (see fixtures.js).
//
// Added by Claude under the guidance of Michael McClennen.

const { defineConfig } = require('@playwright/test');

module.exports = defineConfig({
	testDir: './tests',
	// The test API server switches its tables for every client at once, so the tests must
	// run one at a time.
	workers: 1,
	fullyParallel: false,
	timeout: 60000,
	reporter: [['list']],
	use: {
		baseURL: process.env.PBDB_BASE_URL || 'http://paleobiodb.local',
		launchOptions: {
			args: ['--host-resolver-rules=MAP paleobiodb.local 127.0.0.1'],
		},
		trace: 'retain-on-failure',
		actionTimeout: 10000,
	},
});
