#!/usr/bin/env node

var pubSubLib =  require("./lib/pubsub/");

var done = false;

const pubSubCHannel = new pubSubLib.PubSub(true, "amqp://guest:guest@localhost:5672");

console.log("Preparing to receive message from consumers");

pubSubCHannel.afterChannelOpened(function(channel){
    pubSubCHannel.consume(channel, function(err, msg){
        if (err) {
            console.error("AMQP", err.message);
            throw err;
        }
        else{
            console.log('Received Msg: %s', msg);
       }
    })
});


(function wait () {
    if (!done) setTimeout(wait, 1000);
})();
