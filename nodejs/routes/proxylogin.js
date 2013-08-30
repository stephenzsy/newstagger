/*
 * GET home page.
 */

'use strict'

var https = require('https');
var http = require('http');
var url = require('url');
//var querystring = require('querystring');

var AWS = require('aws-sdk');
AWS.config.loadFromPath('routes/config.json');
var DynamoDB = new AWS.DynamoDB();

exports.proxy = function (req, res) {
    //console.log(JSON.stringify(req.headers));
    //  console.log("URL: ", req.url);
    var parsedUrl = url.parse(req.url);
    //if (parsedUrl['hostname'].match(/.*wsj\.com/)) {
    //   console.log("URL: ", req.url);
    //}
    //  var targetUrl = querystring.parse(parsedUrl['query'])['url'];
    //  console.log("Target URL: ", targetUrl);
    //  if (typeof(targetUrl) != 'string') {
    //     targetUrl = 'http://www.wsj.com';
    //   }

    // var parsedTargetUrl = url.parse(targetUrl);
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

    function checkCookies() {
        if (req.headers['cookie']) {
            var parsedCookie = parseSetCookieHeader(req.headers['cookie']);
            if (parsedCookie['domain'] && !parsedCookie['domain'].match(/\.wsj\.com$/)) {
                return;
            }
            var cookies = {};
            if (parsedCookie['djcs_auto']) {
                cookies["djcs_auto"] = parsedCookie['djcs_auto'];
            }
            if (parsedCookie['djcs_perm']) {
                cookies["djcs_perm"] = parsedCookie['djcs_perm'];
            }
            if (parsedCookie['djcs_info']) {
                cookies["djcs_info"] = parsedCookie['djcs_info'];
            }
            if (parsedCookie['user_type']) {
                cookies['user_type'] = parsedCookie['user_type'];
            }
            if (parsedCookie['djcs_session']) {
                cookies["djcs_session"] = parsedCookie['djcs_session'];
            }
            if (cookies['djcs_auto'] && cookies['djcs_perm']) {
                DynamoDB.putItem({
                    TableName: 'newstagger_state',
                    Item: {
                        "key": {"S": "wsj_cookies"},
                        "value": { "SS": [
                            "djcs_auto=" + cookies["djcs_auto"],
                            "djcs_perm=" + cookies["djcs_perm"],
                            "user_type=" + cookies["user_type"],
                            "djcs_session=" + cookies["djcs_session"]     ,
                            "djcs_info=" + cookies["djcs_info"]
                        ]}
                    }
                }, function (err, data) {
                    if (err) {
                        console.error(err);
                    }
                });
            }

        }
    }

    checkCookies();

    function handleCookie(parsedCookie) {
        if (parsedCookie['domain'] && !parsedCookie['domain'].match(/\.wsj\.com$/)) {
            return;
        }
        if (parsedCookie['Domain'] && !parsedCookie['Domain'].match(/\.wsj\.com$/)) {
            return;
        }
        console.log(JSON.stringify(parsedCookie));
    }

    function rewriteUrl(response) {
        switch (response.statusCode) {
            case 301:
                response.headers['location'] = '/?url=' + encodeURIComponent(response.headers['location']);
                response.headers['cache-control'] = 'no-cache';
        }
        return response;
    }

    function proxyResponseHandler(cres) {
        // cres = rewriteUrl(cres);
        //console.log('status: ' + cres.statusCode);
        //   console.log('headers: ' + JSON.stringify(cres.headers, undefined, 4));
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
            res.write(chunk);
            // console.log(chunk);
        });

        cres.on('end', function (chunk) {
            res.end(chunk);
        });

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
        console.log("Error: Requested URL: " + req.url);
        console.log(e.message);
        //res.send(500);
        res.end();
    });
    creq.end();
};
