var dataTypeLib =  require("../lib/data-type-channel/");

function Greetings(salutation){
    dataTypeLib.RoutingSlip.call(this);
    this.salutation = salutation;
}

Greetings.prototype = new dataTypeLib.RoutingSlip();

module.exports.Greetings = Greetings;

function EnrichedGreeting(salutation, recipient){
    Greetings.call(this, salutation);
    this.recipient = recipient;

}

EnrichedGreeting.prototype = new Greetings();

module.exports.EnrichedGreeting = EnrichedGreeting;

EnrichedGreeting.prototype.greet = function(){
    return this.salutation + " " + this.recipient;
};

