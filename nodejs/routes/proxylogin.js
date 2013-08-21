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
    var protocol = parsedUrl['protocol'];

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

    function parseSetCookieHeader(header) {
        var parts = header.split(/[;]\s*/);

        var parsed = {};

        parts.forEach(function (p) {
            var eqIndex = p.indexOf('=');
            var key = p.substring(0, eqIndex);
            var value = p.substring(eqIndex + 1);
            parsed[key] = value;
        });

        return parsed;
    }

    function handleCookie(parsedCookie) {
        if (parsedCookie['domain'] && !parsedCookie['domain'].match(/\.wsj\.com$/)) {
            return;
        }

        console.log(JSON.stringify(parsedCookie));
    }

    function proxyResponseHandler(cres) {
        //console.log('status: ' + cres.statusCode);
        //console.log('headers: ' + JSON.stringify(cres.headers));
        if (cres.headers['set-cookie']) {
            cres.headers['set-cookie'].forEach(function (h) {
                handleCookie(parseSetCookieHeader(h));
            });
        }
        if (cres.headers['Set-Cookie']) {
            cres.headers['Set-Cookie'].forEach(function (h) {
                handleCookie(parseSetCookieHeader(h));
            });
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

    }

    var creq = null;
    if (protocol === 'https') {
        creq = https.request(options, proxyResponseHandler);
    } else {
        creq = http.request(options, proxyResponseHandler);
    }
    creq.on('error', function (e) {
        // we got an error, return 500 error to client and log error
        console.log(e.statusCode);
        console.log(e.message);
        //res.send(500);
        res.end();
    });
    creq.end();
};
