#!/usr/bin/env node

var amqp = require('amqplib/callback_api');

const exchangeName = "practical-messaging-pipes";
const invalidMessageExchangeName = "practical-pipes-invalid";

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

            channel.assertExchange(invalidMessageExchangeName, 'direct', {durable:true}, function(err, okj){
                if (err){
                    console.error("AMQP", err.message);
                    throw err;
                }
            });

           let invalidQueueName = "invalid." + me.queueName;

            channel.assertQueue(me.queueName, {durable:false, exclusive:false, autoDelete:false, deadLetterExchange:invalidMessageExchangeName, deadLetterRoutingKey:invalidQueueName }, function(err,ok){
                if (err){
                    console.error("AMQP", err.message);
                    throw err;
                }

            });

           channel.assertQueue(invalidQueueName, {durable:true, exclusive:false, autoDelete:false}, function(err,ok){
                if (err){
                    console.error("AMQP". err.message);
                    throw err;
                }
            });

           channel.bindQueue(invalidQueueName, invalidMessageExchangeName, invalidQueueName, {}, function(err, ok){
                if (err){
                    console.error("AMQP", err.message);
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
    channel.publish(exchangeName, this.queueName, Buffer.from(me.serialize(request)), {persistent:true}, function(err,ok){
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
Consumer.prototype.consume = function(channel, cb){
    var me = this;
    channel.prefetch(1);
    channel.consume(me.queueName, function(msg){
        try {
            const request = me.deserialize(msg.content);
            cb(null, request);
            channel.ack(msg);
        }
        catch(e){
            channel.nack(msg, false, false);
            cb(e, null);
        }
    }, {noAck:false});
};

function Filter(url, inputQueueName, deserialize, outputRoutingKey, serialize){
    this.brokerUrl = url;
    this.inputQueueName = inputQueueName;
    this.deserialize = deserialize;
    this.outputRoutingKey = outputRoutingKey;
    this.serialize = serialize;
}

module.exports.Filter = Filter;

Filter.prototype.afterChannelOpened = afterChannelOpened;

Filter.prototype.filter = function(channel, inCb, outCb){
    var me = this;
    channel.prefetch(1);
    channel.consume(me.inputQueueName, function(msg){
        try {
            const request = me.deserialize(msg.content);
            const output = inCb(null, request);
            channel.ack(msg);
            channel.publish(exchangeName, me.outputRoutingKey, Buffer.from(me.serialize(output)), {persistent:true}, function(err,ok){
                if (err) {
                    console.error("AMQP", err.message);
                    throw err;
                }
                outCb(output);
            });
        }
        catch(e){
            channel.nack(msg, false, false);
            outCb(e, null);
        }
    }, {noAck:false});
};




