var nodemailer = require('nodemailer');
nodemailer.sendmail = '/usr/sbin/sendmail';
var utils = require('../lib/utils');

var fs = require('fs');
send_stat_mail = function(emailaddr, mailbody) {
  var subject = '个股雷达-主题提取-无主题文章'
  var email = require("mailer");
  email.send({
    host: "mail.netgen.com.cn",
    // smtp server hostname
    port: "25",
    // smtp server port
    ssl: false,
    // for SSL support - REQUIRES NODE v0.3.x OR HIGHER
    domain: "localhost",
    // domain used by client to identify itself to server
    to: emailaddr,
    from: "手机证券<noreply@netgen.com.cn>",
    subject: subject,
    body: mailbody,
    authentication: "login",
    // auth login is supported; anything else is no auth
    username: "noreply@netgen.com.cn",
    // username
    password: "hand8888" // password
  },
  function(err, result) {
    if (err) {
      console.log(err);
    }
  });
};
getYesterdayDateString = function() {
  var date = new Date();
  var ms = date.getTime();
  date.setTime(ms - 24 * 60 * 60 * 1000);
  var pad = function(i) {
    if (i < 10) {
      return '0' + i;
    }
    return i;
  };
  return [date.getFullYear(), pad(date.getMonth() + 1), pad(date.getDate())].join('');
};

var filename = '../log/subject.' + getYesterdayDateString() + '.log';
var mail_body = fs.readFileSync(filename);
mail_body = mail_body.toString('utf-8');
//send_stat_mail('sjchen@netgen.com.cn', body);

send_stat_mail('shijie.chen@gmail.com', mail_body);
