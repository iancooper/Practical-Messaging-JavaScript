#!/usr/bin/env node

var dataTypeLib =  require("./lib/data-type-channel/");
var greetingLib = require("./model");

var done = false;

const dataTypeChannel = new dataTypeLib.Consumer("practical.messaging.pipes.EnrichedGreeting", "amqp://guest:guest@localhost:5672", function(message){
    const greeting = new greetingLib.EnrichedGreeting();
    JSON.parse(message, function(key, value) {
        greeting[key] = value;
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
            console.log('Received Msg: %s', greeting.greet());
       }
    })
});


(function wait () {
    if (!done) setTimeout(wait, 1000);
})();
