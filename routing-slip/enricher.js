#!/usr/bin/env node

var dataTypeLib =  require("./lib/data-type-channel/");
var greetingLib = require("./model");

var done = false;

const routingStep = new dataTypeLib.RoutingStep(
    "amqp://guest:guest@localhost:5672",
    "practical.messaging.slip.Greeting",
    function(message){
        const greeting = new greetingLib.Greetings("");
        let currentStep = new dataTypeLib.Step();
        JSON.parse(message, function(key, value) {
            if (greeting.hasOwnProperty(key)) {
                if (key !== 'steps') {
                    greeting[key] = value;
                }
            }
            else{
                if (key === "order"){
                    currentStep.order = value;
                }
                else if (key === "routingKey"){
                    currentStep.routingKey = value;
                    greeting.steps.push(currentStep);
                    currentStep = new dataTypeLib.Step();
                }

            }
        });
        return greeting;
    },
    function(enrichedMessage){
        return JSON.stringify(enrichedMessage);
    }

);

console.log("Preparing to receive message from consumers. Type CRTL+C to exit");

routingStep.afterChannelOpened(function(channel){
    routingStep.doStep(
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
        function (err, enrichedGreeting) {
            if (err){
                console.error("AMQP", err.message);
                throw err;
            }
            else {
                console.log("Message enriched as %s", enrichedGreeting.greet());
            }
        }
    )
});


(function wait () {
    if (!done) setTimeout(wait, 1000);
})();