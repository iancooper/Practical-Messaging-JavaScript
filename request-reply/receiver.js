#!/usr/bin/env node

var dataTypeLib =  require("./lib/request-reply-channel/");
var greetingLib = require("./greetings.js");

var done = false;

const dataTypeChannel = new dataTypeLib.Consumer(
    "practical.messaging.event.Greetings",
    "amqp://guest:guest@localhost:5672",
    function(message){
        const greeting = new greetingLib.Greetings("");
        JSON.parse(message, function(key, value) {
            greeting[key] = value;
        });
        return greeting;
    },
    function(response){
        return JSON.stringify(response);
    }
);

console.log("Preparing to receive message from consumers. Type CRTL+C to exit");

dataTypeChannel.afterChannelOpened(function(channel){
    dataTypeChannel.consume(channel, function(err, greeting){
        if (err) {
            console.error("AMQP", err.message);
            throw err;
        }
        else{
            console.log('Received Msg: %s', greeting.salutation.toString());
            return new greetingLib.GreetingResponse("Thank you for your kind words");
       }
    })
});


(function wait () {
    if (!done) setTimeout(wait, 1000);
})();
