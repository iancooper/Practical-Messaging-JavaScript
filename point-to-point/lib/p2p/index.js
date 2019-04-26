#!/usr/bin/env node

var amqp = require('amqplib/callback_api');

const exchangeName = "test-p2p";  //we use the default exchange where routing key is key name to emulate a point-to-point channel

//queueName - the name of the queue we want to create, which ia also the routing key in the default exchange
//url - the amqp url for the rabbit broker, must begin with amqp or amqps
function P2P(queueName, url) {
    this.queueName = queueName;
    this.brokerUrl = url;
}

module.exports.P2P = P2P;

//cb - the callback to send or receive
P2P.prototype.afterChannelOpened = function(cb){
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

            //we don't usually use this for point-to-point which can be the default exchange
            channel.assertExchange(exchangeName, 'direct', {durable:true}, function (err, ok) {
                if (err){
                    console.error("AMQP", err.message);
                    throw err;
                }
                
            })

            channel.assertQueue(me.queueName, {durable: true, exclusive: false, autoDelete:false}, function(err,ok){
                if (err){
                    console.error("AMQP", err.message);
                    throw err;
                }

            });

            //if we had used the default exchange, we always have a routing key equal to queue name,
            //which would be a more idiomatic way of representing point-to-point
            channel.bindQueue(me.queueName, exchangeName, me.queueName, {}, function(err, ok){
                if (err){
                    console.error("AMQP", err.message);
                    throw err;
                }
            });

            cb(channel);

            setTimeout(function() {
                channel.close();
                conn.close();
            }, 500);
        });
    });

}


P2P.prototype.send = function(channel, message, cb){
    channel.publish(exchangeName, this.queueName, Buffer.from(message), {}, function(err,ok){
       if (err){
            console.error("AMQP", err.message);
            throw err;
        }
        cb()
    });
};

P2P.prototype.receive = function(channel, cb){
    channel.get(this.queueName, {noAck:true}, function(err, msgOrFalse){
        if(err){
            console.error("AMQP", err.message);
        }
        else if (msgOrFalse === false){
            cb({});
        }
        else {
            cb(msgOrFalse);
        }
    });
};



