/*
 * GET home page.
 */

'use strict'

var https = require('https');
var http = require('http');
var url = require('url')

exports.proxy = function (req, res) {
    //console.log(JSON.stringify(req.headers));
    var parsedUrl = url.parse(req.url);

    var options = {
        // host to forward to
        hostname: parsedUrl['hostname'],
        // port to forward to
        port: parsedUrl['port'],
        // path to forward to
        path: req.url,
        // request method
        method: req.method,
        // headers to send
        headers: req.headers
    };
    delete req.headers['host'];
    //console.log('requested: ' + options['hostname'] + options['path']);
    var creq = http.request(options,function (cres) {
        //console.log('status: ' + cres.statusCode);
        //console.log('headers: ' + JSON.stringify(cres.headers));
        var setCookieHeader = cres.headers['set-cookie'];
        if (setCookieHeader) {
            console.log(setCookieHeader);
        }
        res.writeHead(cres.statusCode, cres.headers);

        // wait for data
        cres.on('data', function (chunk) {
            //res.write(chunk, cres.headers['content-encoding']);
            res.write(chunk);
        });

        cres.on('end', function () {
            // console.log('ended');
            res.end();
        })

        cres.on('close', function () {
            // closed, let's end client request as well
            // res.send(cres.statusCode);
            //   console.log("closed");
            res.end();
        });

    }).on('error', function (e) {
            // we got an error, return 500 error to client and log error
            console.log(e.message);
            //res.send(500);
            res.end();
        });
    creq.end();
};
