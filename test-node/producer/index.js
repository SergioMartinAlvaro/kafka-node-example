"use strict"
var avroSchema = {
    name: 'MyAwesomeType',
    type: 'record',
    fields: [{
        name: 'id',
        type: 'string',
    },  {
        name: 'timestamp',
        type: 'double',
    }]
};

var avro = require('avsc');
var type = avro.parse(avroSchema);

var kafka = require('kafka-node');
var HighLevelProducer = kafka.HighLevelProducer;
var KeyedMessage = kafka.KeyedMessage;
var Client = kafka.Client;

var client = new Client('localhost:2181', 'mongo-kafka', {
    sessionTimeOut: 300,
    spinDelay: 100,
    retries: 2
});

client.on('error', function() {
    console.log("Error en la conexion con kafka");
});

var producer = new HighLevelProducer(client);

var values = 10;
var valuesArr = [];
var schemaObj;

    producer.on('ready', function() {
        for(var x = 0; x<values; x++) {
            //En base a los valores del ifc se añadirian al esquema
            avroSchema.id = x+"";
            avroSchema.timestamp = Date.now();
            var messageBuffer = type.toBuffer({
                id: avroSchema.id,
                timestamp: avroSchema.timestamp
            });

            var payload =  [{
             topic: 'mongo-kafka',
             messages: messageBuffer,
             attributes: 1
            }];

            producer.send(payload, function(error, result){
               if(error) {
                   console.error(error);
               } else {
                   var formattedResult = result[0];
                   console.log('result: ', result);
               }
            });     
        }
    });




producer.on('error', function() {
    console.log("algo fallo");
});