function scrollTo(t){$("#"+t).goTo()}function updatePage(){$("body").goTo(),ga("send","pageview",document.location.href)}function notFound(){$("#content").html("404 Not Found")}+function(t){function e(){t(n).remove(),t(i).each(function(e){var n=o(t(this));n.hasClass("open")&&(n.trigger(e=t.Event("hide.bs.dropdown")),e.isDefaultPrevented()||n.removeClass("open").trigger("hidden.bs.dropdown"))})}function o(e){var o=e.attr("data-target");o||(o=e.attr("href"),o=o&&/#/.test(o)&&o.replace(/.*(?=#[^\s]*$)/,""));var n=o&&t(o);return n&&n.length?n:e.parent()}var n=".dropdown-backdrop",i="[data-toggle=dropdown]",s=function(e){t(e).on("click.bs.dropdown",this.toggle)};s.prototype.toggle=function(n){var i=t(this);if(!i.is(".disabled, :disabled")){var s=o(i),a=s.hasClass("open");if(e(),!a){if("ontouchstart"in document.documentElement&&!s.closest(".navbar-nav").length&&t('<div class="dropdown-backdrop"/>').insertAfter(t(this)).on("click",e),s.trigger(n=t.Event("show.bs.dropdown")),n.isDefaultPrevented())return;s.toggleClass("open").trigger("shown.bs.dropdown"),i.focus()}return!1}},s.prototype.keydown=function(e){if(/(38|40|27)/.test(e.keyCode)){var n=t(this);if(e.preventDefault(),e.stopPropagation(),!n.is(".disabled, :disabled")){var s=o(n),a=s.hasClass("open");if(!a||a&&27==e.keyCode)return 27==e.which&&s.find(i).focus(),n.click();var r=t("[role=menu] li:not(.divider):visible a",s);if(r.length){var l=r.index(r.filter(":focus"));38==e.keyCode&&l>0&&l--,40==e.keyCode&&l<r.length-1&&l++,~l||(l=0),r.eq(l).focus()}}}};var a=t.fn.dropdown;t.fn.dropdown=function(e){return this.each(function(){var o=t(this),n=o.data("dropdown");n||o.data("dropdown",n=new s(this)),"string"==typeof e&&n[e].call(o)})},t.fn.dropdown.Constructor=s,t.fn.dropdown.noConflict=function(){return t.fn.dropdown=a,this},t(document).on("click.bs.dropdown.data-api",e).on("click.bs.dropdown.data-api",".dropdown form",function(t){t.stopPropagation()}).on("click.bs.dropdown.data-api",i,s.prototype.toggle).on("keydown.bs.dropdown.data-api",i+", [role=menu]",s.prototype.keydown)}(window.jQuery),+function(t){"use strict";var e=function(o,n){this.$element=t(o),this.options=t.extend({},e.DEFAULTS,n),this.transitioning=null,this.options.parent&&(this.$parent=t(this.options.parent)),this.options.toggle&&this.toggle()};e.DEFAULTS={toggle:!0},e.prototype.dimension=function(){var t=this.$element.hasClass("width");return t?"width":"height"},e.prototype.show=function(){if(!this.transitioning&&!this.$element.hasClass("in")){var e=t.Event("show.bs.collapse");if(this.$element.trigger(e),!e.isDefaultPrevented()){var o=this.$parent&&this.$parent.find("> .panel > .in");if(o&&o.length){var n=o.data("bs.collapse");if(n&&n.transitioning)return;o.collapse("hide"),n||o.data("bs.collapse",null)}var i=this.dimension();this.$element.removeClass("collapse").addClass("collapsing")[i](0),this.transitioning=1;var s=function(){this.$element.removeClass("collapsing").addClass("in")[i]("auto"),this.transitioning=0,this.$element.trigger("shown.bs.collapse")};if(!t.support.transition)return s.call(this);var a=t.camelCase(["scroll",i].join("-"));this.$element.one(t.support.transition.end,t.proxy(s,this)).emulateTransitionEnd(350)[i](this.$element[0][a])}}},e.prototype.hide=function(){if(!this.transitioning&&this.$element.hasClass("in")){var e=t.Event("hide.bs.collapse");if(this.$element.trigger(e),!e.isDefaultPrevented()){var o=this.dimension();this.$element[o](this.$element[o]())[0].offsetHeight,this.$element.addClass("collapsing").removeClass("collapse").removeClass("in"),this.transitioning=1;var n=function(){this.transitioning=0,this.$element.trigger("hidden.bs.collapse").removeClass("collapsing").addClass("collapse")};return t.support.transition?(this.$element[o](0).one(t.support.transition.end,t.proxy(n,this)).emulateTransitionEnd(350),void 0):n.call(this)}}},e.prototype.toggle=function(){this[this.$element.hasClass("in")?"hide":"show"]()};var o=t.fn.collapse;t.fn.collapse=function(o){return this.each(function(){var n=t(this),i=n.data("bs.collapse"),s=t.extend({},e.DEFAULTS,n.data(),"object"==typeof o&&o);i||n.data("bs.collapse",i=new e(this,s)),"string"==typeof o&&i[o]()})},t.fn.collapse.Constructor=e,t.fn.collapse.noConflict=function(){return t.fn.collapse=o,this},t(document).on("click.bs.collapse.data-api","[data-toggle=collapse]",function(e){var o,n=t(this),i=n.attr("data-target")||e.preventDefault()||(o=n.attr("href"))&&o.replace(/.*(?=#[^\s]+$)/,""),s=t(i),a=s.data("bs.collapse"),r=a?"toggle":n.data(),l=n.attr("data-parent"),h=l&&t(l);a&&a.transitioning||(h&&h.find('[data-toggle=collapse][data-parent="'+l+'"]').not(n).addClass("collapsed"),n[s.hasClass("in")?"addClass":"removeClass"]("collapsed")),s.collapse(r)})}(window.jQuery),+function(t){function e(o,n){var i,s=t.proxy(this.process,this);this.$element=t(o).is("body")?t(window):t(o),this.$body=t("body"),this.$scrollElement=this.$element.on("scroll.bs.scroll-spy.data-api",s),this.options=t.extend({},e.DEFAULTS,n),this.selector=(this.options.target||(i=t(o).attr("href"))&&i.replace(/.*(?=#[^\s]+$)/,"")||"")+" .nav li > a",this.offsets=t([]),this.targets=t([]),this.activeTarget=null,this.refresh(),this.process()}e.DEFAULTS={offset:10},e.prototype.refresh=function(){var e=this.$element[0]==window?"offset":"position";this.offsets=t([]),this.targets=t([]);var o=this;this.$body.find(this.selector).map(function(){var n=t(this),i=n.data("target")||n.attr("href"),s=/^#\w/.test(i)&&t(i);return s&&s.length&&[[s[e]().top+(!t.isWindow(o.$scrollElement.get(0))&&o.$scrollElement.scrollTop()),i]]||null}).sort(function(t,e){return t[0]-e[0]}).each(function(){o.offsets.push(this[0]),o.targets.push(this[1])})},e.prototype.process=function(){var t,e=this.$scrollElement.scrollTop()+this.options.offset,o=this.$scrollElement[0].scrollHeight||this.$body[0].scrollHeight,n=o-this.$scrollElement.height(),i=this.offsets,s=this.targets,a=this.activeTarget;if(e>=n)return a!=(t=s.last()[0])&&this.activate(t);for(t=i.length;t--;)a!=s[t]&&e>=i[t]&&(!i[t+1]||e<=i[t+1])&&this.activate(s[t])},e.prototype.activate=function(e){this.activeTarget=e,t(this.selector).parents(".active").removeClass("active");var o=this.selector+'[data-target="'+e+'"],'+this.selector+'[href="'+e+'"]',n=t(o).parents("li").addClass("active");n.parent(".dropdown-menu").length&&(n=n.closest("li.dropdown").addClass("active")),n.trigger("activate")};var o=t.fn.scrollspy;t.fn.scrollspy=function(o){return this.each(function(){var n=t(this),i=n.data("bs.scrollspy"),s="object"==typeof o&&o;i||n.data("bs.scrollspy",i=new e(this,s)),"string"==typeof o&&i[o]()})},t.fn.scrollspy.Constructor=e,t.fn.scrollspy.noConflict=function(){return t.fn.scrollspy=o,this},t(window).on("load",function(){t('[data-spy="scroll"]').each(function(){var e=t(this);e.scrollspy(e.data())})})}(window.jQuery),+function(t){function e(){var t=document.createElement("bootstrap"),e={WebkitTransition:"webkitTransitionEnd",MozTransition:"transitionend",OTransition:"oTransitionEnd otransitionend",transition:"transitionend"};for(var o in e)if(void 0!==t.style[o])return{end:e[o]}}t.fn.emulateTransitionEnd=function(e){var o=!1,n=this;t(this).one(t.support.transition.end,function(){o=!0});var i=function(){o||t(n).trigger(t.support.transition.end)};return setTimeout(i,e),this},t(function(){t.support.transition=e()})}(window.jQuery);var Path={version:"0.8.4",map:function(t){return Path.routes.defined.hasOwnProperty(t)?Path.routes.defined[t]:new Path.core.route(t)},root:function(t){Path.routes.root=t},rescue:function(t){Path.routes.rescue=t},history:{initial:{},pushState:function(t,e,o){Path.history.supported?Path.dispatch(o)&&history.pushState(t,e,o):Path.history.fallback&&(window.location.hash="#"+o)},popState:function(){var t=!Path.history.initial.popped&&location.href==Path.history.initial.URL;Path.history.initial.popped=!0,t||Path.dispatch(document.location.pathname)},listen:function(t){if(Path.history.supported=!(!window.history||!window.history.pushState),Path.history.fallback=t,Path.history.supported)Path.history.initial.popped="state"in window.history,Path.history.initial.URL=location.href,window.onpopstate=Path.history.popState;else if(Path.history.fallback){for(route in Path.routes.defined)"#"!=route.charAt(0)&&(Path.routes.defined["#"+route]=Path.routes.defined[route],Path.routes.defined["#"+route].path="#"+route);Path.listen()}}},match:function(t,e){var o,n,i,s,a,r={},l=null;for(l in Path.routes.defined)if(null!==l&&void 0!==l)for(l=Path.routes.defined[l],o=l.partition(),s=0;s<o.length;s++){if(n=o[s],a=t,n.search(/:/)>0)for(i=0;i<n.split("/").length;i++)i<a.split("/").length&&":"===n.split("/")[i].charAt(0)&&(r[n.split("/")[i].replace(/:/,"")]=a.split("/")[i],a=a.replace(a.split("/")[i],n.split("/")[i]));if(n===a)return e&&(l.params=r),l}return null},dispatch:function(t){var e,o;if(Path.routes.current!==t){if(Path.routes.previous=Path.routes.current,Path.routes.current=t,o=Path.match(t,!0),Path.routes.previous&&(e=Path.match(Path.routes.previous),null!==e&&null!==e.do_exit&&e.do_exit()),null!==o)return o.run(),!0;null!==Path.routes.rescue&&Path.routes.rescue()}},listen:function(){var t=function(){Path.dispatch(location.hash)};""===location.hash&&null!==Path.routes.root&&(location.hash=Path.routes.root),"onhashchange"in window&&(!document.documentMode||document.documentMode>=8)?window.onhashchange=t:setInterval(t,50),""!==location.hash&&Path.dispatch(location.hash)},core:{route:function(t){this.path=t,this.action=null,this.do_enter=[],this.do_exit=null,this.params={},Path.routes.defined[t]=this}},routes:{current:null,root:null,rescue:null,previous:null,defined:{}}};Path.core.route.prototype={to:function(t){return this.action=t,this},enter:function(t){return t instanceof Array?this.do_enter=this.do_enter.concat(t):this.do_enter.push(t),this},exit:function(t){return this.do_exit=t,this},partition:function(){for(var t,e,o=[],n=[],i=/\(([^}]+?)\)/g;t=i.exec(this.path);)o.push(t[1]);for(n.push(this.path.split("(")[0]),e=0;e<o.length;e++)n.push(n[n.length-1]+o[e]);return n},run:function(){var t,e,o=!1;if(Path.routes.defined[this.path].hasOwnProperty("do_enter")&&Path.routes.defined[this.path].do_enter.length>0)for(t=0;t<Path.routes.defined[this.path].do_enter.length;t++)if(e=Path.routes.defined[this.path].do_enter[t](),e===!1){o=!0;break}o||Path.routes.defined[this.path].action()}},$(document).ready(function(){var t=$("#stats").height(),e=parseInt($("body").css("font-size"));$(".statBox").height(t-4*e+"px"),$.fn.goTo=function(){return $("html, body").animate({scrollTop:$(this).offset().top-70+"px"},"fast"),this},Path.listen(),$(".dropdown-menu li").on("click",function(){$(".navbar-toggle").click()})}),Path.map("#/apps").to(function(){$("#content").load("pages/apps.html")}).enter(updatePage),Path.map("#/api").to(function(){$("#content").load("pages/api.html")}).enter(updatePage),Path.map("#/about").to(function(){$("#content").load("pages/about.html")}).enter(updatePage),Path.map("#/contact").to(function(){$("#content").load("pages/contact.html")}).enter(updatePage),Path.map("#/faq").to(function(){$("#content").load("pages/faq.html")}).enter(updatePage),Path.map("#/people").to(function(){$("#content").load("pages/people.html")}).enter(updatePage),Path.map("#/people/#map").to(function(){$("#content").load("pages/people.html",function(){setTimeout(function(){scrollTo("members")},300)})}).enter(updatePage),Path.map("#/").to(function(){$("#content").load("pages/root.html")}).enter(updatePage),Path.root("#/"),Path.rescue(notFound);