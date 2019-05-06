#!/usr/bin/env node

var amqp = require('amqplib/callback_api');

const exchangeName = "practical-messaging-invalid-message";
const invalidMessageExchangeName = "practical-message-invalid";

var afterChannelOpened  = function(cb){
    var me = this;
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

            //we don't usually use this for point-to-point which can be the default exchange
            channel.assertExchange(exchangeName, 'direct', {durable:true}, function (err, ok) {
                if (err){
                    console.error("AMQP", err.message);
                    throw err;
                }

            });

            let invalidQueueName = "invalid." + me.queueName;

            channel.assertQueue(me.queueName, {durable:false, exclusive:false, autoDelete:false, /* TODO: set the invalid meessage queue */ function(err,ok){
                if (err){
                    console.error("AMQP", err.message);
                    throw err;
                }

            });

            channel.bindQueue(me.queueName, exchangeName, me.queueName, {}, function(err, ok){
                if (err){
                    console.error("AMQP", err.message);
                    throw err;
                }
                else{
                    cb(channel);
                }
            });

            //declare a queue for invalid messages off an invalid message exchange
            //messages that we nack without requeue will go here
            // TODO; Declare an invalid message queue exchange, direct and durable
            // TODO: declare an invalid message queue, durable
            // TODO: bind the queue to the exchange


            setTimeout(function() {
                channel.close();
                conn.close();
            }, 500);
        });
    });
};

//queueName - the name of the queue we want to create, which ia also the routing key in the default exchange
//url - the amqp url for the rabbit broker, must begin with amqp or amqps
//serialize - serialize objects of a given type to the message body (in a dynamic language that can see a little pointless)
function Producer(queueName, url, serialize) {
    this.queueName = queueName;
    this.brokerUrl = url;
    this.serialize = serialize;
}

module.exports.Producer = Producer;

//cb - the callback to send or receive
Producer.prototype.afterChannelOpened = afterChannelOpened;


//channel - the RMQ channel to make requests on
//message - the data to serialize
//cb a callback indicating success or failure
Producer.prototype.send = function(channel, request, cb){
    var me = this;
    channel.publish(exchangeName, this.queueName, Buffer.from(me.serialize(request)), {}, function(err,ok){
       if (err){
            console.error("AMQP", err.message);
            throw err;
        }
        cb()
    });
};

//queueName - the name of the queue we want to create, which ia also the routing key in the default exchange
//url - the amqp url for the rabbit broker, must begin with amqp or amqps
function Consumer(queueName, url, deserialize) {
    this.queueName = queueName;
    this.brokerUrl = url;
    this.deserialize = deserialize;
}

module.exports.Consumer = Consumer;

//cb - the callback to send or receive
Consumer.prototype.afterChannelOpened = afterChannelOpened;

//channel - the RMQ channel to make requests on
//cb a callback indicating success or failure
Consumer.prototype.receive = function(channel, cb){
    var me = this;
    channel.get(this.queueName, {noAck:false}, function(err, msgOrFalse){
        if(err){
            console.error("AMQP", err.message);
        }
        else if (msgOrFalse === false){
            cb({});
        }
        else {
            try {
                const request = me.deserialize(msgOrFalse.content);
                cb(null, request);
                channel.ack(msgOrFalse);
            }
            catch(e){
                //TODO: Nack and force a requeue
                cb(e, null);
            }
        }
    });
};



