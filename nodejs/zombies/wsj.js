'use strict';

var Browser = require("zombie")

var browser = new Browser({
    //debug: true,
    proxy: 'http://localhost:3000'
});


var loginUrl = null;
browser.
    visit('http://www.wsj.com').
    then(function () {
        browser.wait(10.0);
        // console.log(browser.text("title"));
        var loginUrl = null;
        browser.queryAll('a.loginClass').forEach(function (element) {
            var href = element.getAttribute('href');
            if (href.match(/^http.*/)) {
                loginUrl = href;
            }
        });
        if (loginUrl != null) {
            return browser.visit(loginUrl);
        }
    }).
    then(function () {
        if (loginUrl == null) {
            return this;
        }
    }).
    then(function () {
        browser.close();
    }).
    fail(function () {
        console.error(JSON.stringify(browser.location, undefined, 2));
        console.error("Failed");
        browser.close();
    });
