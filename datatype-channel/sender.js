#!/usr/bin/env node

var dataTypeLib =  require("./lib/data-type-channel/");
var greetingLib = require("./greetings.js");

var done = false;

const dataTypeChannel = new dataTypeLib.Producer("practical.messaging.datatype.Greetings", "amqp://guest:guest@localhost:5672", function(message){
    return JSON.stringify(message);
});

console.log("Preparing to send message to consumers");

dataTypeChannel.afterChannelOpened(function(channel){
    var greeting = new greetingLib.Greetings("Hello World");
    dataTypeChannel.send(channel, greeting, function(){
        console.log("Message sent!");
        done = true;
    })
});


(function wait () {
    if (!done) setTimeout(wait, 1000);
})();


