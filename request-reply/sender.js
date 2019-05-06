#!/usr/bin/env node

var dataTypeLib =  require("./lib/request-reply-channel/");
var greetingLib = require("./greetings.js");

var done = false;

var greeting = new greetingLib.Greetings("Hello World");

const dataTypeChannel = new dataTypeLib.Producer(
    "practical.messaging.event.Greetings",
    "amqp://guest:guest@localhost:5672",
    function(message){
        return JSON.stringify(message);
    },
    function(response){
        const greetingResponse = new greetingLib.GreetingResponse("");
        JSON.parse(response, function(key, value) {
            greeting[key] = value;
        });
        return greetingResponse;
    }
);

console.log("Preparing to send message to consumers");

dataTypeChannel.afterChannelOpened(function(channel){
    const greeting = new greetingLib.Greetings("Hello World");
    dataTypeChannel.call(channel, greeting, function(){
        console.log("Message exchanged!");
        done = true;
    })
});


(function wait () {
    if (!done) setTimeout(wait, 1000);
})();


