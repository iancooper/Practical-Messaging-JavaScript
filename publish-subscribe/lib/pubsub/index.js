#!/usr/bin/env node

var amqp = require('amqplib/callback_api');

const exchangeName = "practical-messaging-pubsub";  //we use the default exchange where routing key is key name to emulate a point-to-point channel

//queueName - the name of the queue we want to create, which ia also the routing key in the default exchange
// if the queue name is null, we assume that you are the publisher, and will not create a queue
//url - the amqp url for the rabbit broker, must begin with amqp or amqps
function PubSub(subscriber, url) {
    this.subscriber = subscriber;
    this.brokerUrl = url;
}

module.exports.PubSub = PubSub;

//cb - the callback to send or receive
PubSub.prototype.afterChannelOpened = function(cb){
    me = this;
    amqp.connect(me.brokerUrl, function(err, conn) {
        if (err) {
            console.error("[AMQP]", err.message);
            throw err;
        }

        conn.createConfirmChannel(function(err, channel){
            if (err) {
                console.error("AMDP", err.message);
                throw err;
            }

            //We are using a fanout exchange which deliver any message sent to it to all queues bound, regardless of routing key
            channel.assertExchange(exchangeName, 'fanout', {durable:true}, function (err, ok) {
                if (err){
                    console.error("AMQP", err.message);
                    throw err;
                }
                
            });

            //if we are subscriber we need to create a queue - we can't if we are the publisher, as we don't know who
            //the subscribers are
            if (me.subscriber) {

                let queueName = '';
                //we don't pass a queuename, as we are not sharing the queue with others, so we just let RMQ give us
                //a random queue name
                channel.assertQueue(queueName, {
                    durable: false,
                    exclusive: false,
                    autoDelete: false
                }, function (err, ok) {
                    if (err) {
                        console.error("AMQP", err.message);
                        throw err;
                    }
                    else{
                        queueName = ok.queue;
                    }

                });

                // we can bind the queue to an empty routing key because there is no routing on a fanout exchange
                channel.bindQueue(queueName, exchangeName, '', {}, function (err, ok) {
                    if (err) {
                        console.error("AMQP", err.message);
                        throw err;
                    }
                });
            }

            cb(channel);

            setTimeout(function() {
                channel.close();
                conn.close();
            }, 500);
        });
    });
};


//channel - the RMQ channel to make requests on
//message - the data to serialize
//cb a callback indicating success or failure
//note how we don't publish to a key, everyone on the exchange gets the message
PubSub.prototype.send = function(channel, message, cb){
    channel.publish(exchangeName, "", Buffer.from(message), {}, function(err,ok){
       if (err){
            console.error("AMQP", err.message);
            throw err;
        }
        cb()
    });
};

//channel - the RMQ channel to make requests on
//cb a callback indicating success or failure
PubSub.prototype.consume = function(channel, cb){
    var me = this;
    channel.prefetch(1);
    channel.consume(me.queueName, function(msg){
        try {
            cb(null, msg);
            channel.ack(msg);
        }
        catch(e){
            channel.nack(msg, false, false);
            cb(e, null);
        }
    }, {noAck:false});
};



