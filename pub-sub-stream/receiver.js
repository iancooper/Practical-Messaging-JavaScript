const { Kafka } = require('kafkajs');
const mysql = require('mysql');

const kafka = new Kafka({
    clientId: 'biographies-consumer',
    brokers: ['localhost:9092'], 
});

const consumer = kafka.consumer({ groupId: 'biographies-group' });

const dbConnection = mysql.createConnection({
    host: 'localhost',
    user: 'root',
    password: 'root',
    database: 'Lookup',
});

const connectToDb = () => {
    return new Promise((resolve, reject) => {
        dbConnection.connect((err) => {
            if (err) {
                reject(err);
            } else {
                console.log('Connected to MySQL database');
                resolve();
            }
        });
    });
};

const saveBiographyToDb = (name, biography) => {
    return new Promise((resolve, reject) => {
        const sql = 'INSERT INTO biographies (name, biography) VALUES (?, ?)';
        dbConnection.query(sql, [name, biography], (err, results) => {
            if (err) {
                reject(err);
            } else {
                console.log(`Biography for ${name} saved to MySQL database`);
                resolve(results);
            }
        });
    });
};

const runConsumerLoop = async () => {
    await consumer.connect();
    await consumer.subscribe({ topic: 'Pub-Sub-Stream-Biography', fromBeginning: true });

    await connectToDb();

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            const name = message.key.toString();
            const biography = message.value.toString();

            try {
                await saveBiographyToDb(name, biography);
                await consumer.commitOffsets([{ topic, partition, offset: message.offset + 1 }]);
            } catch (error) {
                console.error(`Error saving biography for ${name} to MySQL: ${error.message}`);
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
