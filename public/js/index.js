$(document).ready(function() {
  var height = $("#stats").height(),
      em = parseInt($("body").css("font-size"));
  $(".statBox").height(height - em*4 + "px");

  /* Used for scrolling
     Via http://stackoverflow.com/questions/4801655/how-to-go-to-a-specific-element-on-page
  */
  $.fn.goTo = function() {
    $('html, body').animate({
        scrollTop: $(this).offset().top - 70 + 'px'
    }, 'fast');
    return this;
  }

  // Attach the route listener
  Path.listen();


  $('.dropdown-menu li').on('click', function(){
      $(".navbar-toggle").click();
  });

});

// Helper for page scrolling
function scrollTo(element) {
  $("#" + element).goTo();
}

function updatePage() {
  $('body').goTo();
  // update Google Analytics
  ga('send', 'pageview', document.location.href);
}
function notFound(){
  $("#content").html("404 Not Found");
}

Path.map("#/apps").to(function(){
    $("#content").load("pages/apps.html");
}).enter(updatePage);

Path.map("#/api").to(function(){
    $("#content").load("pages/api.html");
}).enter(updatePage);

Path.map("#/about").to(function(){
    $("#content").load("pages/about.html");
}).enter(updatePage);

Path.map("#/contact").to(function(){
    $("#content").load("pages/contact.html");
}).enter(updatePage);

Path.map("#/faq").to(function(){
    $("#content").load("pages/faq.html");
}).enter(updatePage);

Path.map("#/people").to(function(){
    $("#content").load("pages/people.html");
}).enter(updatePage);

Path.map("#/people/#map").to(function(){
    $("#content").load("pages/people.html", function() {
      setTimeout(function(){scrollTo("members")}, 300);
    });
}).enter(updatePage);

Path.map("#/").to(function(){
    $("#content").load("pages/root.html");
}).enter(updatePage);

Path.root("#/");

Path.rescue(notFound);