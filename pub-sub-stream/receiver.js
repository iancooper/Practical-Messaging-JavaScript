const { Kafka } = require('kafkajs');
const mysql = require('mysql2/promise'); // Use mysql2/promise for async/await support

const kafka = new Kafka({
    clientId: 'biographies-consumer',
    brokers: ['localhost:9092'], // Update with your Kafka broker's address
});

const consumer = kafka.consumer({ groupId: 'biographies-group' });

const dbConnection = await mysql.createConnection({
    host: 'localhost',
    user: 'root',
    password: 'root',
    database: 'Lookup',
});

/*
const connectToDb = async () => {
    try {
        await dbConnection.connect();
        console.log('Connected to MySQL database');
    } catch (err) {
        console.error(`Error connecting to MySQL database: ${err.message}`);
        throw err;
    }
};

 */

const saveBiographyToDb = async (name, biography) => {
    try {
        const [results] = await dbConnection.query(
            'INSERT INTO Biography (name, biography) VALUES (?, ?)',
            [name, biography]
        );
        console.log(`Biography for ${name} saved to MySQL database`);
        return results;
    } catch (err) {
        console.error(`Error saving biography for ${name} to MySQL: ${err.message}`);
        throw err;
    }
};

const runConsumerLoop = async () => {
    await consumer.connect();
    await consumer.subscribe({ topic: 'Pub-Sub-Stream-Biography', fromBeginning: true });

    //await connectToDb();

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            const name = message.key.toString();
            const biography = message.value.toString();

            try {
                await saveBiographyToDb(name, biography);
                await consumer.commitOffsets([{ topic, partition, offset: message.offset + 1 }]);
            } catch (error) {
                console.error(`Error handling message for ${name}: ${error.message}`);
            }
        },
    });
};

// Run the consumer loop until the user quits
runConsumerLoop().catch((error) => console.error(`Consumer error: ${error.message}`));

process.on('SIGINT', async () => {
    await consumer.disconnect();
    await dbConnection.end();
    console.log('Consumer and database connection closed');
    process.exit(0);
});
