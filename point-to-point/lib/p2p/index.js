#!/usr/bin/env node

var amqp = require('amqplib/callback_api');

const exchangeName = "";  //we use the default exchange where routing key is key name to emulate a point-to-point channel

function P2P(queueName, url) {

    let p2pConn = null;
    let p2pChannel = null;
    amqp.connect(url, function(err, conn) {
        if (err) {
            console.error("[AMQP]", err.message);
            throw err;
        }

        conn.createChannel(function(err, channel){
            if (err) {
                console.error("AMDP", err.message);
                throw err;
            }

            channel.assertQueue(queueName, {durable: false, exclusive: false, autoDelete:false});
            channel.assertBind(queueName, exchangeName);

            p2pConn = conn;
            p2pChannel = channel;
        });
    });

    this.p2pConn = p2pConn;
    this.p2pChannel = p2pChannel;
    this.routingKey = queueName;
}

module.exports.P2P = P2P;

P2P.prototype.send = function(message){
    this.p2pChannel.publish(exchangeName, this.routingKey, Buffer.from(message), {})
};

P2P.prototype.receive = function(){
    this.p2pChannel.get(this.routingKey, {noAck:true});
};

P2P.prototype.close = function(){
    this.p2pChannel.close();
    this.p2pConn.close();
}



