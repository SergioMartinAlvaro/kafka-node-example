var Test = require('../models/MyAwesomeType');

"use strict"
var typeDescription = {
    name: 'MyAwesomeType',
    type: 'record',
    fields: [{
        name: 'id',
        type: 'string'
    },  {
        name: 'timestamp',
        type: 'double'
    }]
};

var avro = require('avsc');
var type = avro.parse(typeDescription);

var kafka = require('kafka-node');
var HighLevelConsumer = kafka.HighLevelConsumer;
var Client = kafka.Client;

var client = new Client('localhost:2181');
var topics = [{
    topic: 'mongo-kafka'
}];

var options = {
    autoCommit: true,
    fetchMaxWaitMs: 1000,
    fetchMaxBytes: 1024 * 1024,
    encoding: 'buffer',
    fromOffset: 'earliest'
};

var consumer = new HighLevelConsumer(client, topics, options);
consumer.on('message', function(message) {
    var buf = new Buffer(message.value, 'binary');
    var decodedMessage = type.fromBuffer(buf.slice(0));
    var test = new Test();
    test.id = decodedMessage.id;
    test.timestamp = decodedMessage.timestamp;
	test.save((err, testStored) => {
		if(err) {
			res.status(500).send({message: "Error saving test data."});
		} else {
			if(!artistStored) {
				res.status(404).send({message: "Test doesnt saved."});
			} else {
				res.status(200).send({testStored: test});
			}
		}
	});
    console.log(decodedMessage);
});

consumer.on('error', function(err){
    console.log('error', err);
})

process.on('SIGINT', function() {
    consumer.close(true, function(){
        process.exit();
    });
});