// Browser tests of every field on the reference editing panel of the References
// application (/app/refs), for adding a reference of each publication type and for updating
// one.
//
// Added by Claude under the guidance of Michael McClennen.
//
// Which fields the panel shows depends on the publication type. For each test, every field
// that is shown is given a value through its control, the reference is saved, and the test
// checks three things:
//
//   - the body the page sends to refs/addupdate;
//   - the values stored in the test reference tables;
//   - the values the panel shows when the reference is opened for editing again.

const { test, expect, testdb, idNo } = require('../fixtures');



// The fields that are not lists of names. Each entry has:
//
//   id      the element is '#' + id
//   kind    text or select
//   send    the name of the field in the request body
//   col     the column it is stored in, if not the same as send
//   add     the value given when adding a reference (a function of the publication type)
//   upd     the value given when updating it
//   keep    true if the field may not be cleared

const FIELDS = [
	{ id: 'add_pyear', kind: 'text', send: 'pubyr', keep: true,
	  add: () => '2001', upd: '2002' },
	{ id: 'language', kind: 'select', send: 'language', keep: true,
	  add: () => 'English', upd: 'French' },
	{ id: 'add_reftitle', kind: 'text', send: 'reftitle', keep: true,
	  add: t => `Browser test of a ${t} reference`, upd: 'Browser test, updated' },
	{ id: 'add_publication', kind: 'text', send: 'pubtitle', keep: true,
	  add: t => `Journal of ${t} tests`, upd: 'Journal of updated tests' },
	{ id: 'add_doi', kind: 'text', send: 'doi',
	  add: () => '10.1000/test.1', upd: '10.1000/test.2' },
	{ id: 'add_isbn', kind: 'text', send: 'isbn',
	  add: () => '978-0-00-000001-1', upd: '978-0-00-000002-2' },
	{ id: 'publisher', kind: 'text', send: 'publisher', keep: true,
	  add: () => 'Test Press', upd: 'Other Press' },
	{ id: 'city', kind: 'text', send: 'pubcity',
	  add: () => 'Madison', upd: 'Chicago' },
	{ id: 'volume', kind: 'text', send: 'pubvol', add: () => '12', upd: '13' },
	{ id: 'number', kind: 'text', send: 'pubno', add: () => '3', upd: '4' },
	{ id: 'pages', kind: 'text', send: 'pages', add: () => '12-16', upd: '101' },
	{ id: 'project_name', kind: 'text', send: 'project_name',
	  add: () => 'Test project', upd: 'Other project' },
	{ id: 'add_comments', kind: 'text', send: 'comments',
	  add: () => 'A test comment', upd: 'Another test comment' },
];

// The authors and editors, given in the form 'Last, F.' and stored with the last name and
// first name separately.

const NAMES = {
	authors: { prefix: 'author', table: 'REFERENCE_AUTHORS',
			   add: [ 'Smith, J. A.', 'Jones, B.', 'Brown, C. D.' ],
			   upd: [ 'Taylor, P.', 'Smith, J. A.' ],
			   clr: [ 'Taylor, P.' ] },
	editors: { prefix: 'book_editor', table: 'REFERENCE_EDITORS',
			   add: [ 'Green, E.', 'White, F. G.', 'Black, H.' ],
			   upd: [ 'Grey, K.' ],
			   clr: [ ] },
};

const TYPES = [ 'journal article', 'book', 'book chapter', 'serial monograph', 'compendium',
				'M.S. thesis', 'Ph.D. thesis', 'abstract', 'guidebook', 'news article',
				'unpublished', 'instcoll' ];

const COLUMNS = 'publication_type,pubyr,language,reftitle,pubtitle,doi,isbn,publisher,' +
	'pubcity,pubvol,pubno,firstpage,lastpage,project_name,comments';


// Return the value of a field for the values 'which' ('add', 'upd', or 'clr') of a
// reference of the publication type 'type'.

function valueOf (f, which, type) {
	if ( which == 'add' ) return f.add(type);
	if ( which == 'upd' ) return f.upd;
	return f.keep ? f.upd : '';
}


// Return the ids of the fields shown on the panel.

async function shownFields (page) {
	const shown = [];
	for ( const f of FIELDS ) {
		if ( await page.locator(`#${f.id}`).isVisible() ) shown.push(f.id);
	}
	return shown;
}


// Set the authors or editors to the list 'names', adding more fields if needed and
// clearing any left over.

async function setNames (page, kind, names) {

	const prefix = NAMES[kind].prefix;
	const button = kind == 'authors' ? '.add_new_author' : '.add_new_book_editor';

	while ( names.length && await page.locator(`#${prefix}_${names.length}`).count() == 0 )
		await page.locator(button).click();

	for ( let i = 1; await page.locator(`#${prefix}_${i}`).count(); i++ ) {
		await page.locator(`#${prefix}_${i}`).fill(names[i-1] || '');
	}
}


// Return the authors or editors shown on the panel.

async function getNames (page, kind) {

	const prefix = NAMES[kind].prefix;
	const names = [];

	for ( let i = 1; await page.locator(`#${prefix}_${i}`).count(); i++ ) {
		const value = await page.locator(`#${prefix}_${i}`).inputValue();
		if ( value ) names.push(value);
	}

	return names;
}


// Set the publication type, and then every field shown to its value for 'which'. Return
// the fields that were shown, and whether the editors were.

async function setAll (page, which, type) {

	await page.locator('#publication_type').selectOption(type);

	const shown = await shownFields(page);

	for ( const f of FIELDS ) {
		if ( ! shown.includes(f.id) ) continue;
		const elt = page.locator(`#${f.id}`);
		if ( f.kind == 'select' ) await elt.selectOption(valueOf(f, which, type));
		else await elt.fill(valueOf(f, which, type));
	}

	await setNames(page, 'authors', NAMES.authors[which]);

	const editors = await page.locator('#book_editor_1').isVisible();

	if ( editors ) await setNames(page, 'editors', NAMES.editors[which]);

	return { shown, editors };
}


// Click the save button, and return the refs/addupdate request and the body of its
// response. A request that gets a caution is answered by the page, which sends it again,
// so every request is checked.

async function save (page) {

	const requests = [];

	page.on('request', r => {
		if ( r.url().includes('/dtest1.2/refs/addupdate.json') ) requests.push(r);
	});

	await page.locator('#add_edit_submit').click();

	await expect.poll(async () => {
		for ( const r of requests ) {
			const response = await r.response();
			if ( response && response.status() == 200 ) return true;
		}
		return false;
	}, { message: 'a successful response from refs/addupdate', timeout: 10000 }).toBe(true);

	const bodies = [];

	for ( const r of requests ) bodies.push(await (await r.response()).json());

	expect(bodies.map(b => b.errors || b.warnings || 'ok'),
		   'every request succeeded the first time').toEqual(['ok']);

	return { request: requests[0], body: bodies[0] };
}


// Return the stored values of a reference, with its authors and editors.

function stored (refNo) {

	const row = testdb('row', 'refs', 'REFERENCE_DATA', refNo, COLUMNS);

	for ( const kind of [ 'authors', 'editors' ] ) {
		const rows = testdb('rows', 'refs', NAMES[kind].table, 'reference_no', refNo,
							'place,lastname,firstname');
		row[kind] = rows.sort((a, b) => a.place - b.place)
			.map(r => r.firstname ? `${r.lastname}, ${r.firstname}` : r.lastname);
	}

	return row;
}


// Return the values that should be stored, for the fields shown.

function expectedStored (which, type, shown, editors) {

	const expected = { publication_type: type };

	for ( const f of FIELDS ) {
		const value = shown.includes(f.id) ? valueOf(f, which, type) : '';
		if ( f.id == 'pages' ) {
			const [ first, last ] = value.split('-');
			expected.firstpage = first || null;
			expected.lastpage = last || null;
		}
		else expected[f.col || f.send] = value || null;
	}

	expected.authors = NAMES.authors[which];
	expected.editors = editors ? NAMES.editors[which] : [];

	return expected;
}


// Normalize a stored row for comparison: empty values are null.

function normalize (row) {
	return Object.fromEntries(Object.entries(row).map(([k, v]) =>
		[ k, v === '' ? null : typeof v == 'number' ? String(v) : v ]));
}


// Open a reference for editing, in a freshly loaded page.

async function openEdit (page, refId) {

	await page.goto('about:blank');
	await page.goto(`/app/refs?debug=api#edit=${refId}`);
	await expect(page.locator('#add_reftitle')).toBeVisible();
	await expect(page.locator('#add_reftitle')).not.toHaveValue('');
}


// Check the values shown when a reference is opened for editing again.

async function checkShown (page, refId, which, type) {

	await openEdit(page, refId);

	await expect(page.locator('#publication_type')).toHaveValue(type);

	const shownNow = await shownFields(page);
	const expected = {}, values = {};

	for ( const f of FIELDS ) {
		if ( ! shownNow.includes(f.id) ) continue;
		expected[f.id] = valueOf(f, which, type);
		values[f.id] = await page.locator(`#${f.id}`).inputValue();
	}

	expected.authors = NAMES.authors[which];
	values.authors = await getNames(page, 'authors');

	if ( await page.locator('#book_editor_1').isVisible() ) {
		expected.editors = NAMES.editors[which];
		values.editors = await getNames(page, 'editors');
	}

	expect(values, 'the values shown when the reference is edited again').toEqual(expected);
}


test.beforeAll(() => {
	testdb('setup', 'refs');
});

test.afterAll(() => {
	testdb('release', 'refs');
});


// Add a reference of each publication type, with every field that type shows.

// Add a reference of the publication type 'type' through the panel, with every field
// that type shows, and check it. Return its identifier.

async function addAndCheck (page, type) {

	await page.goto('/app/refs?debug=api');
	await page.waitForLoadState('networkidle');

	await page.evaluate(() => refapp.new_ref());
	await expect(page.locator('#author_1')).toBeVisible();

	const { shown, editors } = await setAll(page, 'add', type);

	const { request, body } = await save(page);

	// The page sends every field shown, and the authors and editors.

	const expectedBody = { authors: NAMES.authors.add, publication_type: type };

	for ( const f of FIELDS ) {
		if ( shown.includes(f.id) ) expectedBody[f.send] = valueOf(f, 'add', type);
	}

	if ( editors ) expectedBody.editors = NAMES.editors.add;

	expect(request.postDataJSON(), 'the body sent to refs/addupdate').toEqual(expectedBody);
	expect(page.pageErrors, 'errors in the page').toEqual([]);
	expect(await page.alerts(), 'alerts').toEqual([]);

	const refId = body.records[0].oid;

	expect(normalize(stored(idNo(refId))), 'the stored values')
		.toEqual(normalize(expectedStored('add', type, shown, editors)));

	await checkShown(page, refId, 'add', type);

	return refId;
}


// Add a reference of each publication type, with every field that type shows.

for ( const type of TYPES ) {

	test(`add a ${type} with every field`, async ({ appPage: page }) => {
		await addAndCheck(page, type);
	});
}


// The tests below add a book chapter and then change it, one after another.

test.describe('the book chapter', () => {

test.describe.configure({ mode: 'serial' });

let REF_ID;

test('add the book chapter to change', async ({ appPage: page }) => {
	REF_ID = await addAndCheck(page, 'book chapter');
});

// Update every field of the book chapter, and then clear every optional field.

for ( const [ which, prev ] of [ [ 'upd', 'add' ], [ 'clr', 'upd' ] ] ) {

	const label = which == 'upd' ? 'update every field of' : 'clear every optional field of';

	test(`${label} the book chapter`, async ({ appPage: page }) => {

		const refId = REF_ID;
		test.skip(! refId, 'the book chapter was not added');

		await openEdit(page, refId);

		const { shown, editors } = await setAll(page, which, 'book chapter');

		const { request } = await save(page);

		// The page sends the fields that changed, and the authors and editors if they did.

		const expectedBody = { reference_id: refId };

		for ( const f of FIELDS ) {
			if ( ! shown.includes(f.id) ) continue;
			const value = valueOf(f, which, 'book chapter');
			if ( value != valueOf(f, prev, 'book chapter') ) expectedBody[f.send] = value;
		}

		for ( const kind of [ 'authors', 'editors' ] ) {
			if ( JSON.stringify(NAMES[kind][which]) != JSON.stringify(NAMES[kind][prev]) )
				expectedBody[kind] = NAMES[kind][which];
		}

		expect(request.postDataJSON(), 'the body sent to refs/addupdate').toEqual(expectedBody);
		expect(page.pageErrors, 'errors in the page').toEqual([]);
	expect(await page.alerts(), 'alerts').toEqual([]);

		expect(normalize(stored(idNo(refId))), 'the stored values')
			.toEqual(normalize(expectedStored(which, 'book chapter', shown, editors)));

		await checkShown(page, refId, which, 'book chapter');
	});
}


// Change the book chapter to a journal article. The fields a journal article does not
// show are cleared, as they no longer apply.

test('change the book chapter to a journal article', async ({ appPage: page }) => {

	const refId = REF_ID;
	test.skip(! refId, 'the book chapter was not added');

	await openEdit(page, refId);

	const { shown } = await setAll(page, 'upd', 'journal article');

	await save(page);

	expect(normalize(stored(idNo(refId))), 'the stored values')
		.toEqual(normalize(expectedStored('upd', 'journal article', shown, false)));

	await checkShown(page, refId, 'upd', 'journal article');
});

});
