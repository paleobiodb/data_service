// Browser tests of every field on the collection editing panel of the Collections
// application (/app/collections), for adding a collection and for updating one.
//
// Added by Claude under the guidance of Michael McClennen.
//
// The table FIELDS below lists each field on the panel: the tab it is on, the kind of
// control, and a value to give it when adding a collection and another when updating it.
// For each test, every field is set through its control, the collection is saved, and the
// test checks three things:
//
//   - the body the page sends to colls/add or colls/update;
//   - the value stored in the test collections table;
//   - the value the panel shows when the collection is opened for editing again.

const { test, expect, testdb, idNo } = require('../fixtures');

test.describe.configure({ mode: 'serial' });


// Each entry has:
//
//   id      the element is '#edit_' + id
//   tab     the tab of the editing panel the field is on
//   kind    text, select, check (a checkbox), multi (a dropdown of checkboxes that fills
//           a hidden input), size (the size class checkboxes), or interval
//   add     the value given when adding a collection
//   upd     the value given when updating it
//   send    the name of the field in the request body, if not the same as id
//   col     the column it is stored in, if not the same as send
//   sent    a function giving the value sent in the request body, if not the value given
//   stored  a function giving the stored value, if not the value given
//
// Multiple values are given as arrays. The preservation modes and the collection methods
// are each made up from several dropdowns, and are checked as a group (see GROUPS).

const FIELDS = [
	// Basic info
	{ id: 'access_level', tab: 'basic_info_tab', kind: 'select',
	  add: 'database members', upd: 'the public' },
	{ id: 'release_date', tab: 'basic_info_tab', kind: 'select',
	  add: '6 months', upd: 'immediate', stored: null },
	{ id: 'research_group', tab: 'basic_info_tab', kind: 'multi',
	  add: ['decapod', 'GCP'], upd: ['paleobotany'] },
	{ id: 'collection_name', tab: 'basic_info_tab', kind: 'text',
	  add: 'Browser field test', upd: 'Browser field test, updated' },
	{ id: 'collection_aka', tab: 'basic_info_tab', kind: 'text',
	  add: 'Another name', upd: 'A third name' },
	{ id: 'collection_type', tab: 'basic_info_tab', kind: 'select',
	  add: 'taphonomic', upd: 'biostratigraphic' },

	// Geography
	{ id: 'country', tab: 'geography_tab', kind: 'select',
	  add: 'United States', upd: 'Canada' },
	{ id: 'state', tab: 'geography_tab', kind: 'text', add: 'Wisconsin', upd: 'Ontario' },
	{ id: 'county', tab: 'geography_tab', kind: 'text', add: 'Dane', upd: 'Hastings' },
	{ id: 'latdeg', tab: 'geography_tab', kind: 'text', add: '43', upd: '44' },
	{ id: 'latmin', tab: 'geography_tab', kind: 'text', add: '4', upd: '10', coord: 'min' },
	{ id: 'latsec', tab: 'geography_tab', kind: 'text', add: '30', upd: '20', coord: 'min' },
	{ id: 'latdec', tab: 'geography_tab', kind: 'text', add: '', upd: '', coord: 'dec' },
	{ id: 'latdir', tab: 'geography_tab', kind: 'select', add: 'North', upd: 'North' },
	{ id: 'lngdeg', tab: 'geography_tab', kind: 'text', add: '89', upd: '77' },
	{ id: 'lngmin', tab: 'geography_tab', kind: 'text', add: '24', upd: '30', coord: 'min' },
	{ id: 'lngsec', tab: 'geography_tab', kind: 'text', add: '15', upd: '45', coord: 'min' },
	{ id: 'lngdec', tab: 'geography_tab', kind: 'text', add: '', upd: '', coord: 'dec' },
	{ id: 'lngdir', tab: 'geography_tab', kind: 'select', add: 'West', upd: 'West' },
	{ id: 'latlng_basis', tab: 'geography_tab', kind: 'select',
	  add: 'estimated from map', upd: 'stated in text' },
	{ id: 'gps_datum', tab: 'geography_tab', kind: 'select', add: 'WGS84', upd: 'NAD83' },
	{ id: 'geogscale', tab: 'geography_tab', kind: 'select', add: 'outcrop', upd: 'basin' },
	{ id: 'altitude_value', tab: 'geography_tab', kind: 'text', add: '260', upd: '800' },
	{ id: 'altitude_unit', tab: 'geography_tab', kind: 'select', add: 'meters', upd: 'feet' },
	{ id: 'geogcomments', tab: 'geography_tab', kind: 'text',
	  add: 'Geography comment', upd: 'Geography comment, updated' },

	// Stratigraphy
	{ id: 'early_interval', tab: 'stratigraphy_tab', kind: 'interval',
	  add: 'Campanian', upd: 'Santonian', send: 'early_interval', col: 'max_interval_no' },
	{ id: 'late_interval', tab: 'stratigraphy_tab', kind: 'interval',
	  add: 'Maastrichtian', upd: 'Campanian', send: 'late_interval', col: 'min_interval_no' },
	{ id: 'zone_type', tab: 'stratigraphy_tab', kind: 'select',
	  add: 'ammonoid', upd: 'foram' },
	{ id: 'zone', tab: 'stratigraphy_tab', kind: 'text',
	  add: 'Baculites compressus', upd: 'Globotruncana ventricosa' },
	{ id: 'geological_group', tab: 'stratigraphy_tab', kind: 'text',
	  add: 'Montana group', upd: 'Colorado', sent: v => v.replace(/ group$/, '') },
	{ id: 'formation', tab: 'stratigraphy_tab', kind: 'text',
	  add: 'Hell Creek formation', upd: 'Niobrara', sent: v => v.replace(/ formation$/, '') },
	{ id: 'member', tab: 'stratigraphy_tab', kind: 'text',
	  add: 'Lower member', upd: 'Smoky Hill', sent: v => v.replace(/ member$/, '') },
	{ id: 'stratscale', tab: 'stratigraphy_tab', kind: 'select',
	  add: 'member', upd: 'bed' },
	{ id: 'localsection', tab: 'stratigraphy_tab', kind: 'text',
	  add: 'Section A', upd: 'Section B' },
	{ id: 'localbed', tab: 'stratigraphy_tab', kind: 'text', add: '12', upd: '13' },
	{ id: 'localbedunit', tab: 'stratigraphy_tab', kind: 'select', add: 'm', upd: 'ft' },
	{ id: 'localorder', tab: 'stratigraphy_tab', kind: 'select',
	  add: 'bottom to top', upd: 'top to bottom' },
	{ id: 'regionalsection', tab: 'stratigraphy_tab', kind: 'text',
	  add: 'Region A', upd: 'Region B' },
	{ id: 'regionalbed', tab: 'stratigraphy_tab', kind: 'text', add: '3', upd: '4' },
	{ id: 'regionalbedunit', tab: 'stratigraphy_tab', kind: 'select', add: 'ft', upd: 'cm' },
	{ id: 'regionalorder', tab: 'stratigraphy_tab', kind: 'select',
	  add: 'top to bottom', upd: 'no particular order' },
	{ id: 'direct_ma_value', tab: 'stratigraphy_tab', kind: 'text',
	  add: '66.5', upd: '', col: 'direct_ma' },
	{ id: 'direct_ma_error', tab: 'stratigraphy_tab', kind: 'text', add: '0.5', upd: '' },
	{ id: 'direct_ma_unit', tab: 'stratigraphy_tab', kind: 'select', add: 'Ma', upd: '' },
	{ id: 'direct_ma_method', tab: 'stratigraphy_tab', kind: 'select',
	  add: 'Ar/Ar', upd: '' },
	{ id: 'max_ma_value', tab: 'stratigraphy_tab', kind: 'text',
	  add: '', upd: '84', col: 'max_ma' },
	{ id: 'max_ma_error', tab: 'stratigraphy_tab', kind: 'text', add: '', upd: '1.5' },
	{ id: 'max_ma_unit', tab: 'stratigraphy_tab', kind: 'select', add: '', upd: 'Ma' },
	{ id: 'max_ma_method', tab: 'stratigraphy_tab', kind: 'select',
	  add: '', upd: 'astronomical' },
	{ id: 'min_ma_value', tab: 'stratigraphy_tab', kind: 'text',
	  add: '', upd: '72', col: 'min_ma' },
	{ id: 'min_ma_error', tab: 'stratigraphy_tab', kind: 'text', add: '', upd: '0.25' },
	{ id: 'min_ma_unit', tab: 'stratigraphy_tab', kind: 'select', add: '', upd: 'Ma' },
	{ id: 'min_ma_method', tab: 'stratigraphy_tab', kind: 'select',
	  add: '', upd: 'Ar/Ar' },
	{ id: 'stratcomments', tab: 'stratigraphy_tab', kind: 'text',
	  add: 'Stratigraphy comment', upd: 'Stratigraphy comment, updated' },

	// Geology
	{ id: 'lithification1', tab: 'geology_tab', kind: 'select',
	  add: 'lithified', upd: 'poorly lithified', col: 'lithification' },
	{ id: 'minor_lithology1', tab: 'geology_tab', kind: 'multi',
	  add: ['sandy', 'calcareous'], upd: ['silty'], col: 'minor_lithology' },
	{ id: 'lithology1', tab: 'geology_tab', kind: 'select',
	  add: 'sandstone', upd: 'siltstone' },
	{ id: 'lithadj1', tab: 'geology_tab', kind: 'multi',
	  add: ['massive'], upd: ['lenticular', 'tabular'], col: 'lithadj' },
	{ id: 'fossilsfrom1', tab: 'geology_tab', kind: 'check', add: true, upd: false },
	{ id: 'lithification2', tab: 'geology_tab', kind: 'select',
	  add: 'poorly lithified', upd: 'unlithified' },
	{ id: 'minor_lithology2', tab: 'geology_tab', kind: 'multi',
	  add: ['muddy'], upd: ['sandy'] },
	{ id: 'lithology2', tab: 'geology_tab', kind: 'select',
	  add: 'siltstone', upd: 'claystone' },
	{ id: 'lithadj2', tab: 'geology_tab', kind: 'multi',
	  add: ['lenticular'], upd: ['massive'] },
	{ id: 'fossilsfrom2', tab: 'geology_tab', kind: 'check', add: false, upd: true },
	{ id: 'lithdescript', tab: 'geology_tab', kind: 'text',
	  add: 'Lithology description', upd: 'Lithology description, updated' },
	{ id: 'environment', tab: 'geology_tab', kind: 'select',
	  add: 'fluvial-lacustrine indet.', upd: 'lagoonal' },
	{ id: 'tectonic_setting', tab: 'geology_tab', kind: 'select',
	  add: 'rift', upd: 'passive margin' },
	{ id: 'geology_comments', tab: 'geology_tab', kind: 'text',
	  add: 'Geology comment', upd: 'Geology comment, updated' },

	// Preservation
	{ id: 'size_classes', tab: 'preservation_tab', kind: 'size',
	  add: ['macrofossils', 'microfossils'], upd: ['mesofossils'], col: 'assembl_comps' },
	{ id: 'pres_modes', tab: 'preservation_tab', kind: 'multi', group: 'pres_mode',
	  add: ['body', 'cast'], upd: ['trace'] },
	{ id: 'orig_modes', tab: 'preservation_tab', kind: 'multi', group: 'pres_mode',
	  add: ['original calcite'], upd: ['original aragonite'] },
	{ id: 'repl_modes', tab: 'preservation_tab', kind: 'multi', group: 'pres_mode',
	  add: ['replaced with pyrite'], upd: [] },
	{ id: 'special_modes', tab: 'preservation_tab', kind: 'multi', group: 'pres_mode',
	  add: ['amber'], upd: ['coquina'] },
	{ id: 'temporal_resolution', tab: 'preservation_tab', kind: 'select',
	  add: 'snapshot', upd: 'condensed' },
	{ id: 'spatial_resolution', tab: 'preservation_tab', kind: 'select',
	  add: 'autochthonous', upd: 'allochthonous' },
	{ id: 'lagerstatten', tab: 'preservation_tab', kind: 'select',
	  add: 'conservation', upd: 'concentrate' },
	{ id: 'concentration', tab: 'preservation_tab', kind: 'select',
	  add: 'concentrated', upd: 'dispersed' },
	{ id: 'orientation', tab: 'preservation_tab', kind: 'select',
	  add: 'random', upd: 'life position' },
	{ id: 'preservation_quality', tab: 'preservation_tab', kind: 'select',
	  add: 'good', upd: 'poor' },
	{ id: 'abund_in_sediment', tab: 'preservation_tab', kind: 'select',
	  add: 'common', upd: 'rare' },
	{ id: 'sorting', tab: 'preservation_tab', kind: 'select', add: 'well', upd: 'poor' },
	{ id: 'fragmentation', tab: 'preservation_tab', kind: 'select',
	  add: 'occasional', upd: 'extreme' },
	{ id: 'bioerosion', tab: 'preservation_tab', kind: 'select',
	  add: 'none', upd: 'frequent' },
	{ id: 'encrustation', tab: 'preservation_tab', kind: 'select',
	  add: 'frequent', upd: 'none' },
	{ id: 'preservation_comments', tab: 'preservation_tab', kind: 'text',
	  add: 'Preservation comment', upd: 'Preservation comment, updated' },
	{ id: 'collection_coverage', tab: 'preservation_tab', kind: 'multi',
	  add: ['all macrofossils', 'ichnofossils'], upd: ['some genera'] },
	{ id: 'fossil_source', tab: 'preservation_tab', kind: 'multi', group: 'coll_meth',
	  add: ['field collection'], upd: ['private collection'] },
	{ id: 'coll_meth', tab: 'preservation_tab', kind: 'multi', group: 'coll_meth',
	  add: ['bulk', 'surface (float)'], upd: ['core'] },
	{ id: 'prep_meth', tab: 'preservation_tab', kind: 'multi', group: 'coll_meth',
	  add: ['chemical'], upd: ['sieve'] },
	{ id: 'museum', tab: 'preservation_tab', kind: 'multi',
	  add: ['AMNH', 'FMNH'], upd: ['MCZ'] },
	{ id: 'collectors', tab: 'preservation_tab', kind: 'text',
	  add: 'A. Collector', upd: 'B. Collector' },
	{ id: 'collection_dates', tab: 'preservation_tab', kind: 'text',
	  add: '1999', upd: '2001-2002' },
	{ id: 'collection_comments', tab: 'preservation_tab', kind: 'text',
	  add: 'Collection comment', upd: 'Collection comment, updated' },
	{ id: 'taxonomy_comments', tab: 'preservation_tab', kind: 'text',
	  add: 'Taxonomy comment', upd: 'Taxonomy comment, updated' },
];

// The fields made up from several dropdowns: the name in the request body and the
// column, and the dropdowns that make it up, in the order the page joins them.

const GROUPS = {
	pres_mode: ['pres_modes', 'orig_modes', 'repl_modes', 'special_modes'],
	coll_meth: ['coll_meth', 'prep_meth', 'fossil_source'],
};

// The fields that may not be cleared, because a collection must have a value for them or
// because their control has no empty choice. Every other field is given the value 'clr',
// which is empty.

const KEEP = new Set(['access_level', 'release_date', 'collection_name', 'collection_type',
					  'country', 'state', 'latdeg', 'latdir', 'lngdeg', 'lngdir',
					  'latlng_basis', 'early_interval', 'altitude_unit', 'lithology1',
					  'environment', 'size_classes', 'pres_modes']);

for ( const f of FIELDS ) {
	f.clr = KEEP.has(f.id) ? f.upd : f.kind == 'check' ? false : Array.isArray(f.upd) ? [] : '';
}

const COLUMNS = new Set();

for ( const f of FIELDS ) {
	if ( f.group ) COLUMNS.add(f.group);
	else COLUMNS.add(columnOf(f));
}

COLUMNS.add('release_date');


function sendName (f) { return f.send || f.id; }

function columnOf (f) { return f.col || f.send || f.id; }

function isEmpty (v) { return v === '' || v === false || (Array.isArray(v) && ! v.length); }


// Split a comma-separated set value into a sorted list.

function setOf (v) {
	if ( Array.isArray(v) ) return [ ...v ].sort();
	return (v || '').split(',').filter(x => x != '').sort();
}


// Return the number of an interval, from the test API.

async function intervalNo (page, name) {
	const response = await page.request.get(`/dtest1.2/intervals/single.json?name=${name}`);
	const body = await response.json();
	return idNo(body.records[0].oid);
}


// Show a tab of the editing panel.

async function showTab (page, tab) {
	await page.locator(`#${tab}`).click();
	await expect(page.locator(`#${tab}`)).toHaveClass(/tab-active/);
}


// Open any collapsed section that contains an element, by clicking its heading. A section
// 'X' has the heading '#X' and the content '#X_content'.

async function reveal (page, elt) {

	const sections = await elt.evaluate(e => {
		const ids = [];
		for ( let p = e.parentElement; p; p = p.parentElement ) {
			if ( p.id && p.id.endsWith('_content') && p.style.display == 'none' )
				ids.unshift(p.id.replace(/_content$/, ''));
		}
		return ids;
	});

	for ( const id of sections ) await page.locator(`#${id}`).click();
}


// Set the value of a field through its control.

async function setField (page, f, value) {

	const elt = page.locator(`#edit_${f.id}`);

	await reveal(page, f.kind == 'multi' ? page.locator(`#edit_${f.id}_button`) : elt);

	// A disabled field, such as a coordinate field of the format not chosen, cannot be
	// set by the user either.

	if ( f.kind != 'multi' && f.kind != 'size' && await elt.isDisabled() ) return;

	if ( f.kind == 'text' || f.kind == 'interval' ) {
		await elt.fill(value);
		await elt.blur();
	}

	else if ( f.kind == 'select' ) {
		await elt.selectOption(value);
	}

	else if ( f.kind == 'check' ) {
		await elt.setChecked(value);
	}

	else if ( f.kind == 'size' ) {
		for ( const box of await page.locator('input[name=edit_size_classes]').all() ) {
			await box.setChecked(value.includes(await box.getAttribute('value')));
		}
	}

	else if ( f.kind == 'multi' ) {
		const button = page.locator(`#edit_${f.id}_button`);
		const menu = page.locator(`#edit_${f.id}_menu`);
		await button.click();
		await expect(menu).toBeVisible();
		for ( const box of await menu.locator(`input[name=edit_${f.id}_value]`).all() ) {
			await box.setChecked(value.includes(await box.getAttribute('value')));
		}
		await button.click();
	}
}


// Return the value a field shows.

async function getField (page, f) {

	const elt = page.locator(`#edit_${f.id}`);

	if ( f.kind == 'check' ) return elt.isChecked();

	if ( f.kind == 'size' ) {
		const values = [];
		for ( const box of await page.locator('input[name=edit_size_classes]').all() ) {
			if ( await box.isChecked() ) values.push(await box.getAttribute('value'));
		}
		return values.sort();
	}

	if ( f.kind == 'multi' ) {
		const values = [];
		for ( const box of await page.locator(`input[name=edit_${f.id}_value]`).all() ) {
			if ( await box.isChecked() ) values.push(await box.getAttribute('value'));
		}
		return values.sort();
	}

	return elt.inputValue();
}


// Set every field to its value for 'add' or 'upd', tab by tab. The coordinate format is
// chosen from the fields that have values.

async function setAll (page, which) {

	let tab;

	for ( const f of FIELDS ) {

		if ( f.tab != tab ) {
			await showTab(page, f.tab);
			tab = f.tab;
		}

		if ( f.id == 'latdeg' ) {
			const decimal = FIELDS.some(g => g.coord == 'dec' && g[which]);
			await page.locator(decimal ? '#edit_decimal' : '#edit_min_sec').check();
		}

		await setField(page, f, f[which]);
	}
}


// Return the body the page should send, for the values 'which', when the collection
// previously had the values 'prev' (none, for a new collection). A field is sent only if
// its value changed.

function expectedBody (which, prev) {

	const body = {};

	for ( const f of FIELDS ) {

		if ( f.group ) continue;

		const value = f[which];
		const old = prev ? f[prev] : (f.kind == 'check' ? false : f.kind == 'text' ? '' :
									  Array.isArray(value) ? [] : '');

		if ( JSON.stringify(value) == JSON.stringify(old) ) continue;
		if ( ! prev && isEmpty(value) && f.kind != 'select' ) continue;

		let sent = f.sent ? f.sent(value) : value;

		if ( Array.isArray(sent) ) sent = sent.join(',');
		if ( sent === '' && f.kind != 'check' ) sent = null;

		body[sendName(f)] = sent;
	}

	for ( const [ name, ids ] of Object.entries(GROUPS) ) {
		const now = ids.flatMap(id => FIELDS.find(f => f.id == id)[which]);
		const old = prev ? ids.flatMap(id => FIELDS.find(f => f.id == id)[prev]) : [];
		if ( now.join(',') != old.join(',') ) body[name] = now.join(',');
	}

	return body;
}


// Check a request body against the expected one. A multiple value may be sent in any
// order, so these are compared as sets.

function checkBody (sent, expected, label) {

	const norm = body => Object.fromEntries(Object.entries(body).map(([k, v]) => {
		const f = FIELDS.find(f => sendName(f) == k);
		const multi = (f && (f.kind == 'multi' || f.kind == 'size')) || GROUPS[k];
		return [ k, multi && typeof v == 'string' ? setOf(v) : v ];
	}));

	expect(norm(sent), label).toEqual(norm(expected));
}


// Check the stored row against the values 'which'.

async function checkStored (page, collNo, which) {

	const row = testdb('row', 'colls', 'COLLECTION_DATA', collNo, [ ...COLUMNS ].join(','));

	const expected = {}, stored = {};

	for ( const f of FIELDS ) {

		if ( f.group || f.coord && ! f[which] && f[which] !== '' ) continue;

		const col = columnOf(f);
		let value = f[which];

		// The interval columns are not nullable: no interval is stored as 0.

		if ( f.kind == 'interval' ) value = value ? await intervalNo(page, value) : '0';
		else if ( f.stored !== undefined ) continue;
		else if ( f.sent ) value = f.sent(value);

		if ( f.kind == 'check' ) value = value ? 'Y' : null;
		else if ( f.kind == 'multi' || f.kind == 'size' ) value = setOf(value);
		else if ( value === '' ) value = null;

		let got = row[col];

		if ( f.kind == 'multi' || f.kind == 'size' ) got = setOf(got);
		else if ( got === '' ) got = null;
		else if ( typeof got == 'number' ) got = String(got);
		if ( typeof value == 'number' ) value = String(value);

		expected[col] = value;
		stored[col] = got;
	}

	for ( const [ name, ids ] of Object.entries(GROUPS) ) {
		expected[name] = setOf(ids.flatMap(id => FIELDS.find(f => f.id == id)[which]));
		stored[name] = setOf(row[name]);
	}

	expect(stored, 'the stored values').toEqual(expected);

	return row;
}


// Open a collection in the editing panel, and check that every field shows the values
// 'which'.

async function checkShown (page, collId, which) {

	// Load the page afresh: a change to the hash alone would not reload it.

	await page.goto('about:blank');
	await page.goto(`/app/collections?debug=api#edit=${collId}`);

	await expect(page.locator('#edit_collection_name')).toBeVisible();

	const expected = {}, shown = {};

	for ( const f of FIELDS ) {

		if ( f.stored === null ) continue;

		let value = f[which];

		if ( f.sent ) value = f.sent(value);
		if ( Array.isArray(value) ) value = [ ...value ].sort();

		expected[f.id] = value;
		shown[f.id] = await getField(page, f);
	}

	expect(shown, 'the values shown when the collection is edited again').toEqual(expected);
}


// Click the save button, and return the request, its response, and the response body.

async function save (page, operation) {

	const [ request ] = await Promise.all([
		page.waitForRequest(r => r.url().includes(`/dtest1.2/colls/${operation}.json`),
							{ timeout: 10000 }),
		page.locator('input[type=submit][onclick*="save_collection"]').click(),
	]);

	const response = await request.response();

	return { request, response, body: await response.json() };
}


let COLL_ID;

test.beforeAll(() => {
	testdb('setup', 'colls');
});

test.afterAll(() => {
	testdb('release', 'colls');
});


test('add a collection with every field', async ({ appPage: page }) => {

	await page.goto('/app/collections?debug=api');
	await page.waitForLoadState('networkidle');

	await page.evaluate(() => coll_app.new_collection());
	await expect(page.locator('#edit_collection_name')).toBeVisible();

	await setAll(page, 'add');

	const { request, response, body } = await save(page, 'add');

	const sent = request.postDataJSON();

	// The reference is the selected one.

	expect(sent.reference_no || sent.reference_id || sent.reference_add, 'the reference')
		.toMatch(/^(ref:)?5$/);

	delete sent.reference_no;
	delete sent.reference_id;
	delete sent.reference_add;

	checkBody(sent, expectedBody('add'), 'the body sent to colls/add');

	expect(response.status(), JSON.stringify(body.errors || body.warnings)).toBe(200);
	expect(body.warnings || [], 'warnings in the response').toEqual([]);
	expect(page.pageErrors, 'errors in the page').toEqual([]);
	expect(await page.alerts(), 'alerts').toEqual([]);

	COLL_ID = body.records[0].collection_no;

	const row = await checkStored(page, idNo(COLL_ID), 'add');

	expect(row.release_date, 'a release date six months away').toBeTruthy();

	await checkShown(page, COLL_ID, 'add');
});


// Open the collection for editing, set every field to the values 'which', save it, and
// check the request, the stored values, and the values shown afterwards. The collection
// previously had the values 'prev'.

async function updateAll (page, which, prev) {

	await page.goto(`/app/collections?debug=api#edit=${COLL_ID}`);
	await expect(page.locator('#edit_collection_name')).toBeVisible();

	await setAll(page, which);

	const { request, response, body } = await save(page, 'update');

	const sent = request.postDataJSON();

	expect(sent.collection_id).toBe(COLL_ID);
	delete sent.collection_id;

	checkBody(sent, expectedBody(which, prev), 'the body sent to colls/update');

	expect(response.status(), JSON.stringify(body.errors || body.warnings)).toBe(200);
	expect(body.warnings || [], 'warnings in the response').toEqual([]);
	expect(page.pageErrors, 'errors in the page').toEqual([]);
	expect(await page.alerts(), 'alerts').toEqual([]);

	await checkStored(page, idNo(COLL_ID), which);

	await checkShown(page, COLL_ID, which);
}


test('update every field of the collection', async ({ appPage: page }) => {

	test.skip(! COLL_ID, 'the collection was not added');

	await updateAll(page, 'upd', 'add');
});


test('clear every optional field of the collection', async ({ appPage: page }) => {

	test.skip(! COLL_ID, 'the collection was not added');

	await updateAll(page, 'clr', 'upd');
});


// A collection whose coordinates are in degrees, minutes, and seconds can be changed to
// decimal degrees. The page sends only the decimal degrees, and the API clears the minutes
// and seconds, which it does not accept together with decimal degrees.

test('change the coordinates to decimal degrees', async ({ appPage: page }) => {

	test.skip(! COLL_ID, 'the collection was not added');

	await page.goto(`/app/collections?debug=api#edit=${COLL_ID}`);
	await expect(page.locator('#edit_collection_name')).toBeVisible();

	// Give the collection minutes and seconds first.

	await showTab(page, 'geography_tab');
	await page.locator('#edit_min_sec').check();
	await page.locator('#edit_latmin').fill('10');
	await page.locator('#edit_latsec').fill('20');
	await page.locator('#edit_lngmin').fill('30');
	await page.locator('#edit_lngsec').fill('45');

	let { response, body } = await save(page, 'update');

	expect(response.status(), JSON.stringify(body.errors)).toBe(200);

	// Then change to decimal degrees.

	await page.goto('about:blank');
	await page.goto(`/app/collections?debug=api#edit=${COLL_ID}`);
	await expect(page.locator('#edit_collection_name')).toBeVisible();

	await showTab(page, 'geography_tab');
	await page.locator('#edit_decimal').check();
	await page.locator('#edit_latdec').fill('345');
	await page.locator('#edit_lngdec').fill('25');

	let request;
	({ request, response, body } = await save(page, 'update'));

	expect(response.status(), JSON.stringify(body.errors || body.warnings)).toBe(200);
	expect(body.warnings || [], 'warnings in the response').toEqual([]);

	const row = testdb('row', 'colls', 'COLLECTION_DATA', idNo(COLL_ID),
					   'latmin,latsec,latdec,lngmin,lngsec,lngdec');

	expect(row, `the stored coordinates, after sending ${request.postData()}`).toEqual({
		latmin: null, latsec: null, latdec: '345',
		lngmin: null, lngsec: null, lngdec: '25' });
});


// The page removes the words 'group', 'formation', and 'member' from the end of the
// stratigraphic names, whether or not they are capitalized, as they usually are.

test('stratigraphic names with capitalized suffixes', async ({ appPage: page }) => {

	test.skip(! COLL_ID, 'the collection was not added');

	await page.goto(`/app/collections?debug=api#edit=${COLL_ID}`);
	await expect(page.locator('#edit_collection_name')).toBeVisible();

	await showTab(page, 'stratigraphy_tab');
	await page.locator('#edit_geological_group').fill('Montana Group');
	await page.locator('#edit_formation').fill('Hell Creek Formation');
	await page.locator('#edit_member').fill('Lower Member');

	const { request } = await save(page, 'update');

	const sent = request.postDataJSON();

	expect({ geological_group: sent.geological_group, formation: sent.formation,
			 member: sent.member })
		.toEqual({ geological_group: 'Montana', formation: 'Hell Creek', member: 'Lower' });
});
