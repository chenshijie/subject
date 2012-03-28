var mysql = require('mysql');

var MySqlClient = function(options) {
    var self = this;
    self.client = mysql.createClient(options);
  };

MySqlClient.prototype.get_microblog_content = function(table_name, micro_blog_id, cb) {
  this.client.query('SELECT * FROM ' + table_name + ' WHERE id = ?', [micro_blog_id], function(err, results) {
    if (err) {
      console.log(err);
      cb(err);
    } else {
      cb(results);
    }
  });
};

MySqlClient.prototype.get_article_content = function(acticleid_id, cb) {
  var table_name = 'spider.article_content'
  this.client.query('SELECT * FROM ' + table_name + ' WHERE id = ?', [acticleid_id], function(err, results) {
    if (err) {
      console.log(err);
      cb(err);
    } else {
      cb(results);
    }
  });
};

MySqlClient.prototype.getStockNameAndStockCode = function(cb) {
  this.client.query('SELECT * FROM stock_list', [], function(err, results) {
    if (err) {
      console.log(err);
      cb([]);
    } else {
      cb(results);
    }
  });
};

MySqlClient.prototype.getBlockNameAndBlockid = function(cb) {
  this.client.query('SELECT * FROM block', [], function(err, results) {
    if (err) {
      console.log(err);
      cb([]);
    } else {
      cb(results);
    }
  });
};

MySqlClient.prototype.getBlockAndStock = function(cb) {
  this.client.query('SELECT * FROM block_stock', [], function(err, results) {
    if (err) {
      console.log(err);
      cb([]);
    } else {
      cb(results);
    }
  });
};


MySqlClient.prototype.save2ArticleSubject = function(micro_blog_id, article_id, stock_code, block_id, send_type, in_time, cb) {
  this.client.query('INSERT INTO weibo.article_subject set micro_blog_id = ?,article_id = ?,stock_code =?,block_id = ?,send_type = ?,in_time =?', [micro_blog_id, article_id, stock_code, block_id, send_type, in_time], function(err, results) {
    if (err) {
      console.log(err);
      cb(0);
    } else {
      cb(results.insertId);
    }
  });
};

exports.MySqlClient = MySqlClient;
