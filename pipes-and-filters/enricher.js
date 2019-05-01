#!/usr/bin/env node

var dataTypeLib =  require("./lib/data-type-channel/");
var greetingLib = require("./model");

var done = false;

const filter = new dataTypeLib.Filter(
    "amqp://guest:guest@localhost:5672",
    "practical.messaging.pipes.Greetings",
    function(message){
        const greeting = new greetingLib.Greetings("");
        JSON.parse(message, function(key, value) {
            greeting.salutation = value;
        });
        return greeting;
    },
    "practical.messaging.pipes.EnrichedGreeting",
    function(enrichedMessage){
        return JSON.stringify(enrichedMessage);
    }

);

console.log("Preparing to receive message from consumers. Type CRTL+C to exit");

filter.afterChannelOpened(function(channel){
    filter.filter(
        channel,
        function(err, greeting){
            if (err) {
                console.error("AMQP", err.message);
                throw err;
            }
            else{
                console.log('Received Msg: %s', greeting.salutation.toString());
                return new greetingLib.EnrichedGreeting(greeting.salutation, "Clarissa Harlowe");
           }
        },
        function (enrichedGreeting) {
            console.log("Message enriched as %s", enrichedGreeting.greet())
        }
    )
});


(function wait () {
    if (!done) setTimeout(wait, 1000);
})();