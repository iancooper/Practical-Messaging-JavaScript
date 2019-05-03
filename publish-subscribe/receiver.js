#!/usr/bin/env node

var pubSubLib =  require("./lib/pubsub/");

var done = false;

const subscriberChannel = new pubSubLib.Subscriber("amqp://guest:guest@localhost:5672");

console.log("Preparing to receive message from consumers");

subscriberChannel.afterChannelOpened(function(channel){
    subscriberChannel.consume(channel, function(err, msg){
        if (err) {
            console.error("AMQP", err.message);
            throw err;
        }
        else{
            console.log('Received Msg: %s', msg.content.toString());
       }
    })
});


(function wait () {
    if (!done) setTimeout(wait, 1000);
})();
