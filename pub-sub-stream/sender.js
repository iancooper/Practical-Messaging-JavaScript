const { Kafka } = require('kafkajs');

const kafka = new Kafka({
    clientId: 'biographies-producer',
    brokers: ['localhost:9092'], // Update with your Kafka broker's address
});

const biographies = [
    {name: "Clarissa Harlow", biography: "A young woman whose quest for virtue is continually thwarted by her family."},
    {name: "Pamela Andrews", biography: "A young woman whose virtue is rewarded."},
    {name: "Harriet Byron", biography: "An orphan, and heir to a considerable fortune of fifteen thousand pounds."},
    {name: "Charles Grandison", biography: "A man of feeling who truly cannot be said to feel."},
];

const produceMessage = async (name, biography) => {
    const producer = kafka.producer();

    await producer.connect();

    await producer.send({
        topic: 'Pub-Sub-Stream-Biography',
        messages: [
            {
                key: name,
                value: biography,
            },
        ],
    });

    await producer.disconnect();
};

const main = async () => {
    for (const { name, biography } of biographies) {
        await produceMessage(name, biography);
        console.log(`Message sent for ${name}`);
    }
};

main().catch(console.error);
