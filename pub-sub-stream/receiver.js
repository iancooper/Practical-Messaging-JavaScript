const { Kafka } = require('kafkajs');
const mysql = require('mysql2');

// TODO: Create a Kafka Factory (Hint: Kafka) and set
// Client Id
// Bootstrap server => localhost:9092

//TODO: Create a consumer from the factory - set the group id to 'biographies-group'

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
        const sql = 'INSERT INTO Biography (id, description) VALUES (?, ?)';
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

    //TODO: Connect to the consumer
    //TODO: subscribe the consumer to the 'Pub-Sub-Stream-Biography' topic, from the beginning

    await connectToDb();

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            const name = message.key.toString();
            const biography = message.value.toString();

            try {
                await saveBiographyToDb(name, biography);
                //TODO: Once we have saved the data away, commit the offset to Kafka
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
