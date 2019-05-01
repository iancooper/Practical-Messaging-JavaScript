#!/usr/bin/env node

var dataTypeLib =  require("./lib/data-type-channel/");
var greetingLib = require("./model");

var done = false;

var greeting = new greetingLib.Greetings("Hello World");

const dataTypeChannel = new dataTypeLib.Producer("practical.messaging.pipes." + greeting.constructor.name, "amqp://guest:guest@localhost:5672", function(message){
    return JSON.stringify(message);
});

console.log("Preparing to send message to consumers. Type CTRL + C to exit.");

dataTypeChannel.afterChannelOpened(function(channel){
    let counter = 0;
    (function loop () {
        counter = counter + 1;
        dataTypeChannel.send(channel, "Hello World " + "#" + counter, function () {
            console.log("Message " + counter + " sent!");
        });

        setTimeout(loop, 1000)
    })();


});


(function wait () {
    if (!done) setTimeout(wait, 1000);
})();


