var format = "%s %s some data string to: %s";
var a = "adfkjafgksjgfksjsgkjsf;gjsgfjs;fdg";
var b = "jadfjadfhjadfkjadfkjakdfjadkfjakdfjakfj";
var c = 5;

var NTIMES = 100000;

// use each test seperately - to test memory.

regexTest();
arrayJoinTest();
stringAddTest();

function regexTest() {
    var data = [a, b, c];
    var s = new Date();
    for (var i = 0; i < NTIMES; i++) {
        var k = 0;
        var str = format.replace(/%[sdj]/g, function (match) {
            switch (match) {
            case "%s": return new String(data[k++]);
            case "%d": return new Number(data[k++]);
            case "%j": return JSON.stringify(data[k++]);
            default:
                return match;
            }
        });
    }
    var e = new Date();
    console.log("regex time: %s", e - s);
    console.log("regex memory: %s", require("util").inspect(process.memoryUsage()));
}

function stringAddTest() {
    var data = [a, b, c], str, f;
    var s = new Date();
    for (var i = 0; i < NTIMES; i++) {
        ['%s', '%d', '%j'].forEach(function(separator) {
            f = format.split(separator);
            if (f[0] != format) {
                str = "";
                for (var j = 0, len = f.length - 1; j < len; j++) {
                    str += f[j];
                    switch (separator) {
                        case "%s": str += new String(data[j]);
                        case "%d": str += new Number(data[j]);
                        case "%j": str += JSON.stringify(data[j]);
                        default:
                        str += separator;
                    }
                }
                str += f[f.length - 1];
            }
        });
    }
    var e = new Date();
    console.log("stringAdd time: %s", e - s);
    console.log("stringAdd memory: %s", require("util").inspect(process.memoryUsage()));
}

function arrayJoinTest() {
    var data = [a, b, c], f, str;
    var s = new Date();
    var arr = [ ];

    for (var i = 0; i < NTIMES; i++) {
        ['%s', '%d', '%j'].forEach(function(separator) {
            f = format.split(separator);
            if (f[0] != format) {
                str = "";
                for (var j = 0, len = f.length - 1; j < len; j++) {
                    arr.push(f[j]);
                    switch (separator) {
                        case "%s": arr.push(new String(data[j]));
                        case "%d": arr.push(new Number(data[j]));
                        case "%j": arr.push(JSON.stringify(data[j]));
                        default:
                        arr.push(separator);
                    }
                }
                arr.push(f[f.length - 1]);
                arr.join("");
            }
        });
    }
    var e = new Date();
    console.log("arrayJoin time: %s", e - s);
    console.log("arrayJoin memory: %s", require("util").inspect(process.memoryUsage()));
}
