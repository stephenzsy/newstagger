/**
 * Module dependencies.
 */

var express = require('express');
var routes = require('./routes');
var user = require('./routes/user');
var proxylogin = require('./routes/proxylogin')
var http = require('http');
var path = require('path');

//var app = express();

// all environments
//app.set('port', process.env.PORT || 3000);
//app.set('views', __dirname + '/views');
//app.set('view engine', 'jade');
//app.use(express.favicon());
//app.use(express.logger('dev'));
//app.use(express.bodyParser());
//app.use(express.methodOverride());
//app.use(app.router);
//app.use(express.static(path.join(__dirname, 'public')));

// development only
//if ('development' == app.get('env')) {
//    app.use(express.errorHandler());
//}

//app.get('/', proxylogin.proxy);
//app.get('/users', user.list);
//app.get('/proxylogin/wsj', proxylogin.wsj)

http.createServer(function (req, res) {
    proxylogin.proxy(req, res);
}).listen(3000);
console.log('NodeJS server listening on port 3000');
