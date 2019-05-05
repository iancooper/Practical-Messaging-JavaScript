#!/usr/bin/env node

var dataTypeLib =  require("./lib/data-type-channel/");
var greetingLib = require("./model");

var done = false;


const dataTypeChannel = new dataTypeLib.Producer("practical.messaging.slip.Greeting", "amqp://guest:guest@localhost:5672", function(message){
    return JSON.stringify(message);
});

console.log("Preparing to send message to consumers. Type CTRL + C to exit.");

dataTypeChannel.afterChannelOpened(function(channel){
    let counter = 0;
    (function loop () {
        counter = counter + 1;

        const greeting = new greetingLib.Greetings("Hello World " + "#" + counter);
        greeting.steps.push(new dataTypeLib.Step(1, "practical.messaging.slip.EnrichedGreeting"));

        dataTypeChannel.send(channel, greeting, function () {
            console.log("Message " + counter + " sent!");
        });

        setTimeout(loop, 1000)
    })();
});


(function wait () {
    if (!done) setTimeout(wait, 1000);
})();


