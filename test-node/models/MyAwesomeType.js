"use strict"

var mongoose = require('mongoose');
var Schema = mongoose.Schema;

var TestSchema = Schema({
	id: String,
	timestamp: Date
});

module.exports = mongoose.model('Test', TestSchema);