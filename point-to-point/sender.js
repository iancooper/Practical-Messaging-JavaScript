#!/usr/bin/env node

var p2pLib =  require("./lib/p2p/");

const p2pChannel = new p2pLib.P2P("hello-p2p", "amqp://guest:guest@localhost:5672");

p2pChannel.send("Hello World");

