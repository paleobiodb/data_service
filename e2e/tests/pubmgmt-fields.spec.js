// Browser tests of every field of the publication management page (/app/pubmgmt), for
// adding an official publication, updating it, clearing its optional fields, and deleting
// it.
//
// Added by Claude under the guidance of Michael McClennen.
//
// The page is opened with 'debug=api', so that it lists the publications, loads them for
// editing, and saves them through the test data service. Each test checks the body the
// page sends to pubs/addupdate, the stored values, and the values the form shows when the
// publication is loaded for editing again.

const { test, expect, testdb, idNo } = require('../fixtures');

test.describe.configure({ mode: 'serial' });

// Managing official publications needs administrative permission on the publication table.

test.use({ session: 'SESSION-SUPERUSER' });


// The fields of the form. Each entry has the element id, the name of the field in the
// request body and of the column it is stored in, and a value for adding, updating, and
// clearing. The title, authors, and journal are required.

const FIELDS = [
	{ id: 'inputtitle', name: 'title', add: 'A browser test of official publications',
	  upd: 'A browser test, updated', clr: 'A browser test, updated' },
	{ id: 'inputauthors', name: 'authors', add: 'J. Doe, J.J. Doer',
	  upd: 'A. Smith', clr: 'A. Smith' },
	{ id: 'inputjournal', name: 'journal', add: 'Journal of Tests',
	  upd: 'Journal of Updated Tests', clr: 'Journal of Updated Tests' },
	{ id: 'inputyear', name: 'year', add: '2020', upd: '2021', clr: '' },
	{ id: 'inputvolume', name: 'volume', add: '12', upd: '13', clr: '' },
	{ id: 'inputno', name: 'no', add: '3', upd: '4', clr: '' },
	{ id: 'inputfirst', name: 'first_page', add: '101', upd: '201', clr: '' },
	{ id: 'inputlast', name: 'last_page', add: '110', upd: '210', clr: '' },
	{ id: 'inputdoi', name: 'doi', add: '10.1000/pub.1', upd: '10.1000/pub.2', clr: '' },
];

const COLUMNS = FIELDS.map(f => f.name).join(',');


// Open the page, and check that it reads the publications from the test data service.

async function openPage (page) {

	await page.goto('about:blank');

	const [ list ] = await Promise.all([
		page.waitForRequest(r => r.url().includes('pubs/list.larkin'), { timeout: 10000 }),
		page.goto('/app/pubmgmt?debug=api'),
	]);

	expect(list.url(), 'the list comes from the test data service').toContain('/dtest1.2/');

	await expect(page.locator('#inputtitle')).toBeVisible();
	await page.waitForLoadState('networkidle');
}


// Load a publication into the form for editing, by clicking its edit icon in the list.

async function loadForEdit (page, pubNo) {

	const row = page.locator('#pubTable tr', { has: page.locator(`text="${pubNo}"`) });

	await row.hover();
	await row.locator('i.submitpub').click();
	await expect(page.locator('#pub_no')).toHaveValue(String(pubNo));
}


// Set every field to its value for 'which', click Save, and return the request and the
// body of its response.

async function saveAll (page, which) {

	for ( const f of FIELDS ) await page.locator(`#${f.id}`).fill(f[which]);

	const [ request ] = await Promise.all([
		page.waitForRequest(r => r.url().includes('/dtest1.2/pubs/addupdate.json'),
							{ timeout: 10000 }),
		page.getByRole('button', { name: 'Save' }).click(),
	]);

	const response = await request.response();

	return { request, response, body: await response.json() };
}


// Return the fields of a form-encoded request body.

function formFields (request) {
	return Object.fromEntries(new URLSearchParams(request.postData() || ''));
}


// Return the values 'which' as they should be sent and stored.

function expected (which) {
	return Object.fromEntries(FIELDS.map(f => [ f.name, f[which] ]));
}


// Return the stored values of a publication, with empty values as ''.

function stored (pubNo) {
	const row = testdb('row', 'pubs', 'PUBLICATION_DATA', pubNo, COLUMNS);
	return Object.fromEntries(Object.entries(row).map(([k, v]) => [ k, v == null ? '' : String(v) ]));
}


// Check the values the form shows when the publication is loaded for editing again.

async function checkShown (page, pubNo, which) {

	await openPage(page);
	await loadForEdit(page, pubNo);

	const shown = {};

	for ( const f of FIELDS ) shown[f.name] = await page.locator(`#${f.id}`).inputValue();

	expect(shown, 'the values shown when the publication is edited again').toEqual(expected(which));
}


let PUB_NO;

test.beforeAll(() => {
	testdb('setup', 'pubs');
});

test.afterAll(() => {
	testdb('release', 'pubs');
});


test('add a publication with every field', async ({ appPage: page }) => {

	await openPage(page);

	const { request, response, body } = await saveAll(page, 'add');

	expect(request.url(), 'a new publication is added with allow=CREATE').toMatch(/allow=CREATE/);
	expect(formFields(request), 'the body sent to pubs/addupdate')
		.toEqual({ ...expected('add'), pub_no: '' });

	expect(response.status(), JSON.stringify(body.errors || body.warnings)).toBe(200);
	expect(body.warnings || [], 'warnings in the response').toEqual([]);

	PUB_NO = idNo(String(body.records[0].oid || body.records[0].pub_no));

	await expect.poll(() => page.alerts()).toEqual([ `New publication ${PUB_NO}` ]);
	await expect(page.locator('#pub_no')).toHaveValue(PUB_NO);
	expect(page.pageErrors, 'errors in the page').toEqual([]);

	expect(stored(PUB_NO), 'the stored values').toEqual(expected('add'));

	await checkShown(page, PUB_NO, 'add');
});


for ( const [ which, label ] of [ [ 'upd', 'update every field of' ],
								  [ 'clr', 'clear every optional field of' ] ] ) {

	test(`${label} the publication`, async ({ appPage: page }) => {

		test.skip(! PUB_NO, 'the publication was not added');

		await openPage(page);
		await loadForEdit(page, PUB_NO);

		const { request, response, body } = await saveAll(page, which);

		expect(request.url(), 'an update is sent without allow=CREATE').not.toMatch(/allow=CREATE/);
		expect(formFields(request), 'the body sent to pubs/addupdate')
			.toEqual({ ...expected(which), pub_no: PUB_NO });

		expect(response.status(), JSON.stringify(body.errors || body.warnings)).toBe(200);
		expect(body.warnings || [], 'warnings in the response').toEqual([]);

		await expect.poll(() => page.alerts()).toEqual([ `Updated publication ${PUB_NO}` ]);
		expect(page.pageErrors, 'errors in the page').toEqual([]);

		expect(stored(PUB_NO), 'the stored values').toEqual(expected(which));

		await checkShown(page, PUB_NO, which);
	});
}


test('delete the publication', async ({ appPage: page }) => {

	test.skip(! PUB_NO, 'the publication was not added');

	await openPage(page);
	await loadForEdit(page, PUB_NO);

	const [ request ] = await Promise.all([
		page.waitForRequest(r => r.url().includes('/dtest1.2/pubs/delete.json'), { timeout: 10000 }),
		page.getByRole('button', { name: 'Delete' }).click(),
	]);

	expect(new URL(request.url()).searchParams.get('pub_no')).toBe(PUB_NO);
	expect((await request.response()).status()).toBe(200);

	expect(page.dialogs, 'the confirmation').toEqual([ `Delete official publication ${PUB_NO}?` ]);
	await expect.poll(() => page.alerts()).toEqual([ `Publication ${PUB_NO} was deleted.` ]);

	expect(testdb('row', 'pubs', 'PUBLICATION_DATA', PUB_NO, 'pub_no'), 'the publication is gone')
		.toEqual({});

	await expect(page.locator('#pubTable')).not.toContainText(`${PUB_NO}`);
});
