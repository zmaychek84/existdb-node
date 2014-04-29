/*global module, require */

var http = require("http");
var util = require("util");
var events = require("events");

function Query(xquery, config, options) {
    var self = this;
    this.xquery = xquery;
    this.variables = {};
    this.config = config;
    this.options = {
        chunkSize: 10
    };
    if (options) {
        for (var name in options) {
            self.options[name] = options[name];
        }
    }
    this.postOptions = {
        host: config.host,
        port: config.port,
        method: "POST",
        path: config.rest + "/db/",
        auth: config.auth || "guest:guest"
    };
}

util.inherits(Query, events.EventEmitter);

module.exports = Query;

Query.prototype.bind = function(name, value) {
    this.variables[name] = value;
    return this;
};

Query.prototype.each = function(callback) {
    var self = this;

    var offset = 1;
    var hits = 0;
    var sessionId = null;

    function nextChunk() {
        var options = {
            start: offset,
            max: self.options.chunkSize,
            method: "json"
        };

        if (sessionId) {
            options.session = sessionId;
        } else {
            options.cache = "yes";
        }

	    var postBody = createPostBody(options, self.xquery, self.variables);
        var req = http.request(self.postOptions, function(res) {
            var received = "";

	        res.setEncoding("UTF-8");

	        res.on("data", function(data) {
                received += data;
            });

	        res.on("error", function(e) {
                self.emit("error", e);
            });

	        res.on("end", function() {
                if (res.statusCode != 200) {
                    var error = new Error(received);
                    error.statusCode = res.statusCode;
                    self.emit("error", error);
                    return;
                }

                var data = JSON.parse(received);
                hits = data.hits;
                sessionId = data.session;

                if (Array.isArray(data.data)) {
                    var i = 0;
                    data.data.forEach(function (item) {
                        callback(item, hits, offset + i++);
                    });
                } else {
	                // because of weird exist serialization behaviour: if no results, exist returns null;
	                // if single result, exist returns object instead of array
	                if (data.count === 0) {
		                callback([], hits, offset);
	                } else {
		                callback(data.data, hits, offset);
	                }
                }
                offset = data.start + data.count;
                if (offset < hits) {
                    nextChunk();
                } else {
	                self.emit("end", "done");
                }
            });
        });

        req.write(postBody);
        req.end();
    }

    nextChunk();
};

function createPostBody(options, query, variables) {
    var body = "<query xmlns=\"http://exist.sourceforge.net/NS/exist\" ";
    Object.keys(options).forEach(function(key) {
        body += key + "=\"" + options[key] + "\" ";
    });
    body += ">\n";
    if (variables) {
        body += "<variables>";
        Object.keys(variables).forEach(function(variable) {
            body += "<variable xmlns:sx=\"http://exist-db.org/xquery/types/serialized\">" +
                "<qname><localname>" + variable + "</localname></qname>" +
                serialize(variables[variable]) +
                "</variable>";
        });
        body += "</variables>";
    }
    body += "   <text><![CDATA[" + query + "]]></text>\n";
    body += "</query>";
    return body;
}

function serialize(data) {
    var sequence = "<sx:sequence>";
    if (Array.isArray(data)) {
        data.forEach(function(item) {
            sequence += serializeValue(item);
        });
    } else {
        sequence += serializeValue(data);
    }
    sequence += "</sx:sequence>";
    return sequence;
}

function serializeValue(value) {
    var type = "xs:string";
    if (typeof value === "number") {
        if (value % 1 != 0) {
            type = "xs:double";
        } else {
            type = "xs:integer";
        }
    }
    return "<sx:value type=\"" + type + "\">" + value + "</sx:value>";
}