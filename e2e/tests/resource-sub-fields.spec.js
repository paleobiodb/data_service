// Browser tests of every field of the resource submission page (/app/resource-sub) and of
// the resource manager (/app/resource-sub/manager), for submitting a resource, updating
// it, clearing its optional fields, and activating, deactivating, and deleting it.
//
// Added by Claude under the guidance of Michael McClennen.
//
// Each test checks the body the page sends to eduresources/addupdate, the values stored in
// the test resource tables, and the values the form shows when the resource is loaded for
// editing again. Both pages require a login to Classic, which the fixture provides with its
// test cookie.

const { test, expect, testdb, idNo } = require('../fixtures');

test.describe.configure({ mode: 'serial' });


// A 2x2 PNG image, to upload as the resource image.

const PNG = Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAIAAAACCAYAAABytg0kAAAAEklEQVR4nGP4z8DA' +
						'wMDAwMAAABKAAf+zxNYAAAAASUVORK5CYII=', 'base64');

const PNG_DATA = 'data:image/png;base64,' + PNG.toString('base64');

// The tag numbers of the resource types and audiences, from the table 'edutags'.

const TAG_NO = { mobile: 1, lesson_plan: 2, tutorial: 3, data_entry: 4, R: 5, presentation: 6,
				 API: 7, web: 8, other: 9, general: 100, primary: 101, secondary: 102,
				 'ug-intro': 103, 'ug-adv': 104, grad: 105, other_aud: 106 };


// The text fields. Each entry has the selector of the element, the name of the field in the
// request body and of the column it is stored in, and a value for submitting, updating, and
// clearing. The title, URL, and author are required.

const TEXT = [
	{ sel: '#res-title', name: 'title', add: 'A browser test resource',
	  upd: 'A browser test resource, updated', clr: 'A browser test resource, updated' },
	{ sel: '#res-desc', name: 'description', add: 'A resource added by the browser tests.',
	  upd: 'An updated description.', clr: '' },
	{ sel: '#res-url', name: 'url', add: 'https://example.org/resource-1',
	  upd: 'https://example.org/resource-2', clr: 'https://example.org/resource-2' },
	{ sel: 'input[name=auth-name]', name: 'author', add: 'A. Tester',
	  upd: 'B. Tester', clr: 'B. Tester' },
	{ sel: 'input[name=auth-email]', name: 'email', add: 'tester@example.org',
	  upd: 'other@example.org', clr: '' },
	{ sel: 'input[name=auth-affil]', name: 'affil', add: 'Test University',
	  upd: 'Other University', clr: '' },
	{ sel: 'input[name=auth-ORCID]', name: 'orcid', add: '0000-0001-2345-6789',
	  upd: '0000-0002-3456-7890', clr: '' },
	{ sel: 'input[name=res-topics]', name: 'topics', add: 'extinction',
	  upd: 'diversity', clr: '' },
	{ sel: 'input[name=res-taxa]', name: 'taxa', add: 'Dinosauria',
	  upd: 'Mammalia', clr: '' },
	{ sel: 'input[name=res-timestart]', name: 'timestart', add: 'Cretaceous',
	  upd: 'Paleogene', clr: '' },
	{ sel: 'input[name=res-timeend]', name: 'timeend', add: 'Paleogene',
	  upd: '', clr: '' },
];

// The resource type (a radio button) and audiences (checkboxes), and whether to upload the
// image or keep the one already stored. The page has no way to remove an image.

const CHOICES = {
	add: { type: 'web', audience: [ 'general', 'grad' ], image: 'upload' },
	upd: { type: 'tutorial', audience: [ 'ug-intro' ], image: 'keep' },
	clr: { type: 'tutorial', audience: [ ], image: 'keep' },
};


// Return the timespan the page makes from the start and end.

function timespan (which) {
	const start = TEXT.find(f => f.name == 'timestart')[which];
	const end = TEXT.find(f => f.name == 'timeend')[which];
	return end ? `${start} - ${end}` : start;
}


// Open a page, and wait for its list of resources to load.

async function openPage (page, path) {

	await page.goto('about:blank');
	await page.goto(`${path}?debug=api`);
	await expect(page.locator('#res-title')).toBeVisible();
	await page.waitForLoadState('networkidle');
}


// Set every field to its value for 'which'.

async function setAll (page, which) {

	for ( const f of TEXT ) await page.locator(f.sel).fill(f[which]);

	const c = CHOICES[which];

	await page.locator(`input[name=res-type][value=${c.type}]`).check();

	for ( const box of await page.locator('input[name=res-aud]').all() ) {
		await box.setChecked(c.audience.includes(await box.getAttribute('value')));
	}

	if ( c.image == 'upload' ) await uploadImage(page);
}


// Upload the test image.

async function uploadImage (page) {

	await page.locator('#inputFileToLoad').setInputFiles({ name: 'test.png',
														   mimeType: 'image/png',
														   buffer: PNG });
}


// Return the body the page should send for the values 'which'.

function expectedBody (which) {

	const body = {};

	for ( const f of TEXT ) {
		if ( f.name != 'timestart' && f.name != 'timeend' ) body[f.name] = f[which];
	}

	const c = CHOICES[which];

	// The page joins the type and the audiences with a comma even when there are no
	// audiences, which leaves a trailing comma. ResourceEdit ignores it.

	body.tags = c.type + ',' + c.audience.join(',');
	body.is_video = false;
	body.timespan = timespan(which);

	if ( c.image == 'upload' ) body.image_data = PNG_DATA;

	return body;
}


// Return the values that should be stored in the queue table for the values 'which'.

function expectedStored (which, status) {

	const row = { status };

	for ( const f of TEXT ) {
		if ( f.name != 'timestart' && f.name != 'timeend' ) row[f.name] = f[which] || null;
	}

	const c = CHOICES[which];

	row.tags = [ c.type, ...c.audience ].map(t => TAG_NO[t]).sort((a, b) => a - b).join(',');
	row.is_video = '0';
	row.timespan = timespan(which) || null;

	return row;
}


// Return the stored values of a resource in the queue table.

function stored (resNo) {

	const columns = 'status,title,description,url,author,email,affil,orcid,topics,taxa,' +
		'timespan,tags,is_video';

	const row = testdb('row', 'eduresources', 'RESOURCE_QUEUE', resNo, columns);

	row.tags = (row.tags || '').split(',').filter(t => t).map(Number).sort((a, b) => a - b).join(',');

	return Object.fromEntries(Object.entries(row).map(([k, v]) =>
		[ k, v === '' ? null : typeof v == 'number' ? String(v) : v ]));
}


// Return the values the form shows.

async function shownValues (page) {

	const shown = {};

	for ( const f of TEXT ) shown[f.name] = await page.locator(f.sel).inputValue();

	shown.type = await page.locator('input[name=res-type]:checked').getAttribute('value')
		.catch(() => null);

	shown.audience = [];

	for ( const box of await page.locator('input[name=res-aud]:checked').all() )
		shown.audience.push(await box.getAttribute('value'));

	shown.image = await page.locator('#imgTest img').getAttribute('src').catch(() => null);

	return shown;
}


// Return the values the form should show for the values 'which'.

function expectedShown (which) {

	const shown = {};

	for ( const f of TEXT ) shown[f.name] = f[which];

	const c = CHOICES[which];

	shown.type = c.type;
	shown.audience = c.audience;
	shown.image = PNG_DATA;

	return shown;
}


// Load one of the user's resources into the submission form, through the dialog that
// lists them.

async function editOnSubmissionPage (page, resId) {

	await page.getByText('Click here to edit one of the').click();
	await page.locator(`#userResources input[name=resource][value="${resId}"]`).check();
	await page.locator('#userResources input[type=submit]').click();
	await expect(page.locator('#res-title')).not.toHaveValue('');
}


// Load a resource into the manager's form, from the list in the specified tab.

async function editOnManagerPage (page, resId, tab) {

	await page.locator(`a[href="#${tab}"]`).click();
	await page.locator(`#${tab}List`).selectOption(resId);
	await page.getByRole('button', { name: 'Edit Resource' }).click();
	await expect(page.locator('#res-title')).not.toHaveValue('');
}


let RES_ID;

test.beforeAll(() => {
	testdb('setup', 'eduresources');
});

test.afterAll(() => {
	testdb('release', 'eduresources');
});


test.describe('the submission page', () => {

	test('submit a resource with every field', async ({ appPage: page }) => {

		await openPage(page, '/app/resource-sub');

		await setAll(page, 'add');

		const [ request ] = await Promise.all([
			page.waitForRequest(r => r.url().includes('/dtest1.2/eduresources/addupdate.json'),
								{ timeout: 10000 }),
			page.getByRole('button', { name: 'Submit' }).click(),
		]);

		expect(request.method()).toBe('PUT');

		const sent = request.postDataJSON();
		const image = sent.image;
		delete sent.image;

		expect(sent, 'the body sent to eduresources/addupdate').toEqual(expectedBody('add'));
		expect(image, 'no default image, since one was uploaded').toBe('');

		const response = await request.response();
		const body = await response.json();

		expect(response.status(), JSON.stringify(body.errors || body.warnings)).toBe(200);
		expect(body.warnings || [], 'warnings in the response').toEqual([]);

		await expect.poll(() => page.alerts())
			.toEqual([ 'Your resource was successfully submitted for approval' ]);
		expect(page.pageErrors, 'errors in the page').toEqual([]);

		RES_ID = body.records[0].oid;

		expect(stored(idNo(RES_ID)), 'the stored values').toEqual(expectedStored('add', 'pending'));

		await openPage(page, '/app/resource-sub');
		await editOnSubmissionPage(page, RES_ID);

		expect(await shownValues(page), 'the values shown when the resource is edited again')
			.toEqual(expectedShown('add'));
	});


	for ( const [ which, prev, label ] of [ [ 'upd', 'add', 'update every field of' ],
											[ 'clr', 'upd', 'clear every optional field of' ] ] ) {

		test(`${label} the resource`, async ({ appPage: page }) => {

			test.skip(! RES_ID, 'the resource was not submitted');

			await openPage(page, '/app/resource-sub');
			await editOnSubmissionPage(page, RES_ID);

			await setAll(page, which);

			const [ request ] = await Promise.all([
				page.waitForRequest(r => r.url().includes('/dtest1.2/eduresources/addupdate.json'),
									{ timeout: 10000 }),
				page.getByRole('button', { name: 'Submit' }).click(),
			]);

			expect(new URL(request.url()).searchParams.get('eduresource_id')).toBe(RES_ID);

			const sent = request.postDataJSON();
			delete sent.image;

			expect(sent, `the body sent to eduresources/addupdate (errors in the page: ` +
				   `${page.pageErrors.join('; ') || 'none'})`).toEqual(expectedBody(which));

			const response = await request.response();
			const body = await response.json();

			expect(response.status(), JSON.stringify(body.errors || body.warnings)).toBe(200);
			expect(body.warnings || [], 'warnings in the response').toEqual([]);
			expect(page.pageErrors, 'errors in the page').toEqual([]);

			// An update by anyone without administrative permission sets the status to
			// 'changes', even for a resource that was never activated.

			expect(stored(idNo(RES_ID)), 'the stored values')
				.toEqual(expectedStored(which, 'changes'));

			await openPage(page, '/app/resource-sub');
			await editOnSubmissionPage(page, RES_ID);

			expect(await shownValues(page), 'the values shown when the resource is edited again')
				.toEqual(expectedShown(which));
		});
	}


	// A new image can be uploaded for a resource that is being edited.

	test('replace the image of a resource being edited', async ({ appPage: page }) => {

		test.skip(! RES_ID, 'the resource was not submitted');

		await openPage(page, '/app/resource-sub');
		await editOnSubmissionPage(page, RES_ID);

		await uploadImage(page);

		const [ request ] = await Promise.all([
			page.waitForRequest(r => r.url().includes('/dtest1.2/eduresources/addupdate.json'),
								{ timeout: 10000 }),
			page.getByRole('button', { name: 'Submit' }).click(),
		]);

		expect(page.pageErrors, 'errors in the page').toEqual([]);
		expect(request.postDataJSON().image_data, 'the new image was sent').toBe(PNG_DATA);
	});


	// A resource submitted without an image, after one submitted with an image, does not get
	// the previous image.

	test('submit a second resource without an image', async ({ appPage: page }) => {

		await openPage(page, '/app/resource-sub');

		const submit = () => Promise.all([
			page.waitForRequest(r => r.url().includes('/dtest1.2/eduresources/addupdate.json'),
								{ timeout: 10000 }),
			page.getByRole('button', { name: 'Submit' }).click(),
		]);

		await setAll(page, 'add');

		const [ first ] = await submit();

		expect(first.postDataJSON().image_data, 'the first resource has the image').toBe(PNG_DATA);
		expect((await first.response()).status()).toBe(200);

		await page.locator('#res-title').fill('A second browser test resource');
		await page.locator('#res-url').fill('https://example.org/resource-3');
		await page.locator('input[name=auth-name]').fill('C. Tester');
		await page.locator('input[name=res-type][value=other]').check();

		const [ second ] = await submit();

		const sent = second.postDataJSON();

		expect(sent.image_data, 'the second resource has no image').toBeUndefined();
		expect(sent.image, 'and gets the default image for its type').toBe('default_other.png');
		expect((await second.response()).status()).toBe(200);
		expect(page.pageErrors, 'errors in the page').toEqual([]);
	});
});


test.describe('the manager', () => {

	// Managing resources needs administrative permission on the resource tables.

	test.use({ session: 'SESSION-SUPERUSER' });

	test('activate the resource with changes', async ({ appPage: page }) => {

		test.skip(! RES_ID, 'the resource was not submitted');

		await openPage(page, '/app/resource-sub/manager');
		await editOnManagerPage(page, RES_ID, 'pending');

		expect(await shownValues(page), 'the values shown by the manager')
			.toEqual(expectedShown('clr'));

		await setAll(page, 'upd');

		const [ request ] = await Promise.all([
			page.waitForRequest(r => r.url().includes('/dtest1.2/eduresources/addupdate.json'),
								{ timeout: 10000 }),
			page.getByRole('button', { name: 'Activate with changes' }).click(),
		]);

		const url = new URL(request.url());

		expect(url.searchParams.get('eduresource_id')).toBe(RES_ID);
		expect(url.searchParams.get('status')).toBe('active');
		expect(request.postDataJSON(), 'the body sent to eduresources/addupdate')
			.toEqual(expectedBody('upd'));

		const response = await request.response();
		const body = await response.json();

		expect(response.status(), JSON.stringify(body.errors || body.warnings)).toBe(200);

		await expect.poll(() => page.alerts())
			.toEqual([ 'Your resource was successfully activated' ]);
		expect(page.pageErrors, 'errors in the page').toEqual([]);

		expect(stored(idNo(RES_ID)), 'the stored values').toEqual(expectedStored('upd', 'active'));

		const active = testdb('row', 'eduresources', 'RESOURCE_ACTIVE', idNo(RES_ID),
							  'title,description,url,author');

		expect(active, 'the active copy').toEqual({ title: TEXT[0].upd, description: TEXT[1].upd,
													url: TEXT[2].upd, author: TEXT[3].upd });

		await openPage(page, '/app/resource-sub/manager');
		await editOnManagerPage(page, RES_ID, 'active');

		expect(await shownValues(page), 'the values shown when the resource is edited again')
			.toEqual(expectedShown('upd'));
	});


	for ( const [ button, status, from, to, message ] of [
		[ 'Deactivate', 'inactive', 'active', 'inactive', 'deactivated' ],
		[ 'Activate', 'active', 'inactive', 'active', 'activated' ] ] ) {

		test(`${button.toLowerCase()} the resource`, async ({ appPage: page }) => {

			test.skip(! RES_ID, 'the resource was not submitted');

			await openPage(page, '/app/resource-sub/manager');

			await page.locator(`a[href="#${from}"]`).click();
			await page.locator(`#${from}List`).selectOption(RES_ID);

			const [ request ] = await Promise.all([
				page.waitForRequest(r => r.url().includes('/dtest1.2/eduresources/addupdate.json'),
									{ timeout: 10000 }),
				page.getByRole('button', { name: button, exact: true }).click(),
			]);

			expect(new URL(request.url()).searchParams.get('status')).toBe(status);
			expect((await request.response()).status()).toBe(200);

			await expect.poll(() => page.alerts())
				.toEqual([ `Your resource was successfully ${message}` ]);

			expect(stored(idNo(RES_ID)).status, 'the stored status').toBe(status);

			await page.locator(`a[href="#${to}"]`).click();
			await expect(page.locator(`#${to}List option[value="${RES_ID}"]`)).toHaveCount(1);
		});
	}


	test('delete the resource', async ({ appPage: page }) => {

		test.skip(! RES_ID, 'the resource was not submitted');

		await openPage(page, '/app/resource-sub/manager');

		await page.locator('a[href="#active"]').click();
		await page.locator('#activeList').selectOption(RES_ID);

		const [ request ] = await Promise.all([
			page.waitForRequest(r => r.url().includes('/dtest1.2/eduresources/delete.json'),
								{ timeout: 10000 }),
			page.getByRole('button', { name: 'Delete', exact: true }).click(),
		]);

		expect(new URL(request.url()).searchParams.get('eduresource_id')).toBe(RES_ID);
		expect((await request.response()).status()).toBe(200);

		expect(page.dialogs, 'the confirmation').toEqual([ `Delete resource ${RES_ID}?` ]);
		await expect.poll(() => page.alerts()).toEqual([ 'Your resource was successfully deleted' ]);

		expect(testdb('row', 'eduresources', 'RESOURCE_QUEUE', idNo(RES_ID), 'status'),
			   'the resource is gone').toEqual({});
		expect(testdb('row', 'eduresources', 'RESOURCE_ACTIVE', idNo(RES_ID), 'title'),
			   'and so is its active copy').toEqual({});
	});
});
