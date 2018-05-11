"use strict"

var Test = require('../models/MyAwesomeType');
var mongoose = require('mongoose');
require('events').EventEmitter.prototype._maxListeners = 0;

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
    fetchMaxWaitMs: 10000,
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
    mongoose.connect('mongodb://localhost:27017/mongotest', { useMongoClient: true });
    var db = mongoose.connection;
    //db.setMaxListeners(1000);
    db.once('open', function() {
        test.save((err, testStored) => {
        	if(!testStored) {
			console.log("Error al guardar los datos");
		} else {
			console.log("Datos guardados satisfactoriamente");
		}
	});

    });
    mongoose.connection.close();
});

consumer.on('error', function(err){
    console.log('error', err);
})

process.on('SIGINT', function() {
    consumer.close(true, function(){
        process.exit();
    });
});