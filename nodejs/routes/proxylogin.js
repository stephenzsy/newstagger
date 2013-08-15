/*
 * GET home page.
 */

'use strict'

var https = require('https');
var http = require('http');
var needle = require('needle');

exports.wsj = function (req, r) {
    needle.get("http://www.wsj.com", {follow: 10}, function (error, response, body) {
        r.send(body);
        r.end();
        console.log(response.headers);
    });
    return;
};
