function verMeenu() {
 var element = document.getElementById("menu-togle");
 //element.classList.toggle("mystyle");
 element.style.display = none;
}

$.get('/stats.txt', { url:""}, function(data) {
  let sumar_lista = 0;
  var lista1 = data.split("\n");
      lista1[0] = "";
      lista1.forEach( function ( element ) {
	if ( element ) {
	  let lista2=element.split(",");
	  lista2 = lista2[lista2.length - 1];
          sumar_lista=parseInt(lista2)+parseInt(sumar_lista);
        }
      });
   $('.Contando_Visitas').html(sumar_lista);
});

$(document).ready(function() {
	$('.Contando_Visitas').each(function () {
	  var $this = $(this);
	  jQuery({ Counter: 0 }).animate({ Counter: $this.attr('data-stop') }, {
	    duration: 1000,
	    easing: 'swing',
	    step: function (now) {
	      // $this.text(Math.ceil(now));
	    }
	  });
	});
});

var chkMenu = document.getElementById("menu-toggle");
var menu = document.getElementById("menu");

$("#menu-toggle").change( function() {
  if ( chkMenu.checked ) {
    $("#menu").show("fast","swing");
  } else {
    $("#menu").hide("slow");
    //menu.style.display = "none";
  }
})
$(function () {
    "use strict";
    var $win = $(window);
	 /*==========   Scroll Top Button   ==========*/

    /*document.querySelector("footer").insertAdjacentHTML('beforeend', 
    '<button id="scrollTopBtn"><i class="fa fa-long-arrow-up"></i></button>'
    );*/
    var $scrollTopBtn = $('#scrollTopBtn');
    // Show Scroll Top Button
    $win.on('scroll', function () {
        if ($(this).scrollTop() > 500) {
            $scrollTopBtn.addClass('actived');
        } else {
          $scrollTopBtn.removeClass('actived');
        }
    });
    // Animate Body after Clicking on Scroll Top Button
    $scrollTopBtn.on('click', function () {
        $('html, body').animate({
            scrollTop: 0
        }, 400);
    });
});


