function Greetings(salutation){
    this.salutation = salutation;
}

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

