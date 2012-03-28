var cp = require('child_process');

var doSegment = function(options, task, cb) {
  var text = task.text;
  if (!text) {
    cb([]);
  }
  text = text.replace(/[\r\n\t\s\|\"\'\<\{\(\)\}\>\;&!]/gm, '');
  //bin/scws -t 3 -M 3 -i "中华人民共和国" -c utf8 -d ./etc/dict.utf8.xdb
  var cmd = options.cmd;
  var dict = " -I -d " + options.dict;

  var input = ' -i ' + text;
  var multi = '';
  if (options.multi) {
    multi = " -M " + options.multi;
  }

  if (!options.top) {
    options.top = 200;
  }

  var top = " -t " + options.top;
  
  if(options.rule) {
    var rule = " -r " + options.rule;
  } else {
    var rule = '';
  }

  var charset = " -c utf8";
  cmd += dict + top + multi + charset + input + rule ;
  var child = cp.exec(cmd, function(err, result) {
    if (err) {
      cb(err);
      return;
    }
    var json = toJson(result);
    cb(json);
  });
}

/*

No. WordString               Attr  Weight(times)
-------------------------------------------------
01. 字符集                n     7.82(1)
02. 词典                   n     5.50(1)
*/

var toJson = function(result) {
  var a = result.split("\n");
  if (a.length < 3) {
    return [];
  }
  var words = [];
  for (var i = 2; i < a.length; i++) {
    var m = a[i].match(/\d+\.\s([^\s]+)\s+(\w+)\s+([\d\.]+)\((\d+)\)/);
    if (!m) {
      continue;
    }
    var word = {
      word: m[1],
      weight: parseFloat(m[3]),
      attr: m[2],
      times: parseInt(m[4])
    };
    words.push(word);
  }
  return words;
}

var Segment = function(options) {
  this.options = options;
}

Segment.prototype.doSegment = function(task, cb) {
  doSegment(this.options, task, cb);
}

exports.getSegment = function(options) {
  return new Segment(options);
};

