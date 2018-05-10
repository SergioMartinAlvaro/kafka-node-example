"use strict"

//Importamos las librerias
var mongoose = require('mongoose');
var app = require('./app');
//Puerto para el servidor web
var port = process.env.PORT || 3977;

//Quitamos el aviso de Mongoose Promise
mongoose.Promise = global.Promise;

//Crea la conexion con mongoDB y la bbdd
mongoose.connect('mongodb://localhost:27017/gitBand', { useMongoClient: true }, (err, res) => {
	if(err) {
		console.log("Database not working");
		throw err;
	} else {
		console.log("Database its running corretly");
		//Pone al servidor a la escucha
		app.listen(port, function(){
			console.log("Api Rest Server hearing in http://localhost:" + port);
		})
	}
});