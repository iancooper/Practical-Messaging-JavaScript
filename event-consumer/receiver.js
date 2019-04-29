#!/usr/bin/env node

var dataTypeLib =  require("./lib/data-type-channel/");
var greetingLib = require("./greetings.js");

var done = false;

const dataTypeChannel = new dataTypeLib.Consumer("practical.messaging.event." + new greetingLib.Greetings().constructor.name, "amqp://guest:guest@localhost:5672", function(message){
    const greeting = new greetingLib.Greetings("");
    JSON.parse(message, function(key, value) {
        greeting.salutation = value;
    });
    return greeting;
});

console.log("Preparing to receive message from consumers. Type CRTL+C to exit");

dataTypeChannel.afterChannelOpened(function(channel){
    dataTypeChannel.consume(channel, function(err, greeting){
        if (err) {
            console.error("AMQP", err.message);
            throw err;
        }
        else{
            console.log('Received Msg: %s', greeting.salutation.toString());
       }
    })
});


(function wait () {
    if (!done) setTimeout(wait, 1000);
})();
