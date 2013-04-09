//
// This script provides code for generating a mapping widget for the Paleobiology Database.
// It requires OpenLayers.js, release 2.12 or later.
//

// First create a new subclass of Openlayers.Control.  This class will be
// instantiated to create a control for intercepting map clicks.

document.write('<script src="http://maps.google.com/maps?file=api&amp;v=2&amp;key=AIzaSyCG1j8oI62sQAXWRutBVCKmo4FvsQuonnw"></script>');

OpenLayers.Control.Click = OpenLayers.Class(OpenLayers.Control, {                
    defaultHandlerOptions: {
        'single': true,
        'double': false,
        'pixelTolerance': 0,
        'stopSingle': false,
        'stopDouble': false
    },
    
    initialize: function(options) {
        this.handlerOptions = OpenLayers.Util.extend(
            {}, this.defaultHandlerOptions
        );
        OpenLayers.Control.prototype.initialize.apply(
            this, arguments
        ); 
        this.handler = new OpenLayers.Handler.Click(
            this, {
                'click': this.trigger
            }, this.handlerOptions
        );
	
	this.click_callback = typeof options.click_callback ? options.click_callback :
	    function(lat,lng) { alert("You clicked near " + lat + " N, " +
				      + lon + " E"); };
    },
    
    trigger: function(e) {
        var lonlat = this.map.getLonLatFromPixel(e.xy);
        this.click_callback(lonlat.lat, lonlat.lon);
    }
    
});



// Create and return a new "map widget" object.  Options accepted include:
// 
// click_callback : the value of this option must be a function.  It will be
//	            called with the arguments (lat, lng) when the user clicks
//	            on the map.

function PBDB_map_widget(mapElementName, options) {
    
    // First, create an OpenLayers map, plus a baselayer and some default
    // controls.
    
    this.map = new OpenLayers.Map(mapElementName, {
	controls: [
	    new OpenLayers.Control.Navigation(),
	    new OpenLayers.Control.PanZoom(),
	    new OpenLayers.Control.LayerSwitcher(),
	    new OpenLayers.Control.MousePosition()
	]});
    
    var gphy = new OpenLayers.Layer.Google(
        "Google Physical",
        {type: G_PHYSICAL_MAP}
    );
    var gmap = new OpenLayers.Layer.Google(
        "Google Streets", // the default
        {numZoomLevels: 20}
    );
    var ghyb = new OpenLayers.Layer.Google(
        "Google Hybrid",
        {type: G_HYBRID_MAP, numZoomLevels: 20}
    );
    var gsat = new OpenLayers.Layer.Google(
        "Google Satellite",
        {type: G_SATELLITE_MAP, numZoomLevels: 22}
    );
    
    this.map.addLayers([gphy, gmap, ghyb, gsat]);
    
    //this.base_layer = new OpenLayers.Layer.WMS( "OpenLayers WMS",
    //				"http://vmap0.tiles.osgeo.org/wms/vmap0", 
    // {layers: 'basic'} );
    //this.map.addLayer(this.base_layer);
    
    // Then create a marker layer and marker which will be used to show the
    // selected coordinates.
    
    var size = new OpenLayers.Size(21,25);
    var offset = new OpenLayers.Pixel(-(size.w/2), -size.h);
    this.marker_icon = new OpenLayers.Icon('http://www.openlayers.org/dev/img/marker.png', size, offset);
    
    this.marker_layer = new OpenLayers.Layer.Markers("Coordinate Marker");
    this.map.addLayer(this.marker_layer);
    
    // Then add a control of the type defined above, which will respond to a
    // map click by calling the value of the option "click_callback".
    
    this.click = new OpenLayers.Control.Click(options);
    this.map.addControl(this.click);
    this.click.activate();
    
    if (!this.map.getCenter()) { this.map.zoomToMaxExtent(); }
    
    // Now define some utility functions
    
    this.deactivateMarker = function () {
	if ( this.marker_object != null) {
	    this.marker_layer.removeMarker(this.marker_object);
	    this.marker_object = null;
	}
    }
    
    this.setMarker = function ( lon, lat ) {
	this.deactivateMarker();
	this.marker_object = 
	    new OpenLayers.Marker(new OpenLayers.LonLat(lon, lat), 
				  this.marker_icon.clone());
	this.marker_layer.addMarker(this.marker_object);
    }
    
    // This function pans the map if necessary to ensure that the marker is
    // visible.  The single parameter, if true, forces the map to center on
    // the marker position.
    
    this.showMarker = function (center) {

	var pos = mw.marker_object.lonlat;
	var port = mw.map.getExtent();
	
	// If the parameter 'force' is true, or if the marker position is not
	// currently visible, then center the map on the marker position.  If
	// 'force' is true, then zoom to level 10 if the current zoom level is
	// less than 10.
	
	if ( center || pos.lon < port.left || pos.lon > port.right ||
	     pos.lat < port.bottom || pos.lat > port.top )
	{
	    if ( center && mw.map.zoom < 10 )
	    {
		mw.map.setCenter(pos, 10);
	    }
	    else
	    {
		mw.map.setCenter(pos);
	    }
	}
    }
}


