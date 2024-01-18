const { Kafka } = require('kafkajs');

// TODO: Create a Kafka Factory (Hint: Kafka) and set
// Client Id
// Bootstrap server => localhost:9092

const biographies = [
    {name: "Clarissa Harlow", biography: "A young woman whose quest for virtue is continually thwarted by her family."},
    {name: "Pamela Andrews", biography: "A young woman whose virtue is rewarded."},
    {name: "Harriet Byron", biography: "An orphan, and heir to a considerable fortune of fifteen thousand pounds."},
    {name: "Charles Grandison", biography: "A man of feeling who truly cannot be said to feel."},
];

const produceMessage = async (name, biography) => {

    //TODO: Create a producer from the factory

    //TODO: Connect to the broker

    //TODO: Send a message to the Pub-Sub-Stream Biography topic
    // With a message whose key is the name and value the biography

    //TODO: Disconnect the producer

};

const main = async () => {
    for (const { name, biography } of biographies) {
        await produceMessage(name, biography);
        console.log(`Message sent for ${name}`);
    }
};

main().catch(console.error);
