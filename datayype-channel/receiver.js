#!/usr/bin/env node

var dataTypeLib =  require("./lib/data-type-channel/");
var greetingLib = require("./greetings.js");

var done = false;

const dataTypeChannel = new dataTypeLib.Consumer("practical.messaging.datatype." + new greetingLib.Greetings().constructor.name, "amqp://guest:guest@localhost:5672", function(message){
//TODO: deserialize the message
});

console.log("Preparing to receive message from consumers");

dataTypeChannel.afterChannelOpened(function(channel){
    dataTypeChannel.receive(channel, function(greeting){
        console.log('Received Msg: %s', greeting.salutation.toString());
        done  = true;
    })
});


(function wait () {
    if (!done) setTimeout(wait, 1000);
})();
