#!/usr/bin/env node

var pubSubLib =  require("./lib/pubsub/");

var done = false;

const publisherChannel = new pubSubLib.Publisher("amqp://guest:guest@localhost:5672");

console.log("Preparing to send message to consumers");

publisherChannel.afterChannelOpened(function(channel){
    publisherChannel.send(channel,"Hello World", function(){
        console.log("Message sent!");
        done = true;
    })
});


(function wait () {
    if (!done) setTimeout(wait, 1000);
})();


