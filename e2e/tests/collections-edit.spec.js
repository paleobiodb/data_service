// Browser tests of editing a collection with the Collections application (/app/collections).
// They check that the page sends colls/update a body the API accepts, containing only the
// fields that were changed, and that the change is stored.
//
// Added by Claude under the guidance of Michael McClennen.

const { test, expect, testdb, idNo } = require('../fixtures');

let COLL_ID;

test.beforeAll(() => {
	({ collection_id: COLL_ID } = testdb('setup', 'colls'));
});

test.afterAll(() => {
	testdb('release', 'colls');
});


// Open the test collection in the edit tab.

async function openEdit (page) {

	await page.goto(`/app/collections?debug=api#edit=${COLL_ID}`);

	const name = page.locator('#edit_collection_name');

	await expect(name).toBeVisible();
	await expect(name).toHaveValue('Browser test collection');

	return name;
}


// Click the save button, and return the colls/update request and its response.

async function save (page) {

	const [ request ] = await Promise.all([
		page.waitForRequest(r => /\/dtest1\.2\/colls\/update\.json/.test(r.url())),
		page.locator('input[type=submit][onclick*="save_collection"]').click(),
	]);

	const response = await request.response();

	return { request, response, body: await response.json() };
}


test('change the collection name', async ({ appPage: page }) => {

	const name = await openEdit(page);

	await name.fill('Browser test collection, renamed');

	const { request, response, body } = await save(page);

	// The page sends the collection id and the one changed field, to the test server.

	expect(request.method()).toBe('POST');
	expect(request.postDataJSON()).toEqual({ collection_id: COLL_ID,
											 collection_name: 'Browser test collection, renamed' });

	expect(response.status()).toBe(200);
	expect(body.warnings || [], 'warnings in the response').toEqual([]);
	// The page asks for vocab=pbdb, so the identifier is in 'collection_no'.

	expect(body.records[0].collection_no).toBe(COLL_ID);

	// The page shows the saved collection in the details tab, without an alert.

	await expect(page.locator('#details_tab')).toHaveClass(/tab-active/);
	expect(page.dialogs).toEqual([]);
	expect(page.pageErrors).toEqual([]);

	// And the change is stored.

	const row = testdb('row', 'COLLECTION_DATA', idNo(COLL_ID), 'collection_name');

	expect(row.collection_name).toBe('Browser test collection, renamed');
});
