var mysql = require('mysql');

var MySqlClient = function(options) {
  var self = this;
  self.client = mysql.createClient(options);
};

MySqlClient.prototype.get_article_content = function(table_name, acticleid_id, cb) {
  this.client.query('SELECT * FROM ' + table_name + ' WHERE id = ?', [url_id], function(err, results) {
    if (err) {
      console.log(err);
      cb(err);
    } else {
      cb(results);
    }
  });
};

exports.MySqlClient = MySqlClient;