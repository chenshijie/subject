var fs = require('fs');
var MySqlClient = require('./lib/mysql').MySqlClient;
var logger = require('./lib/logger').logger;
var configs = require('./etc/settings.json');
var _logger = logger(__dirname + '/' + configs.log.file);
var utils = require('./lib/utils');
var WorkFlow = require('./lib/workflow').WorkFlow;
var Worker = require('./lib/worker');
var queue = require('queuer');
var devent = require('devent').createDEvent('subject');

var databases = {};
var i = 0;
for (i = 0; i < configs.mysql.length; i++) {
  var options = configs.mysql[i];
  var key = options.host + ':' + options.port + ':' + options.database;
  var mysql = new MySqlClient(options);
  databases[key] = mysql;

  console.log(key);
}

// 将pid写入文件，以便翻滚日志时读取
fs.writeFileSync(__dirname + '/run/server.lock', process.pid.toString(), 'ascii');

// 排重队列
console.log('http://' + configs.queue_server.host + ':' + configs.queue_server.port + '/' + configs.queue_server.queue_path, configs.subject_generate_queue);
var queue4Similar = queue.getQueue('http://' + configs.queue_server.host + ':' + configs.queue_server.port + '/' + configs.queue_server.queue_path, configs.subject_generate_queue);
// 文章对列
var queue4Subject = queue.getQueue('http://' + configs.queue_server.host + ':' + configs.queue_server.port + '/' + configs.queue_server.queue_path, configs.subject_monitor_queue);
console.log('http://' + configs.queue_server.host + ':' + configs.queue_server.port + '/' + configs.queue_server.queue_path, configs.subject_monitor_queue);

var SCWSSegment = require('./lib/segment');

devent.on('queued', function(queue) {
  // 同时多个队列进入时会调用allSpidersRun()多次。
  if (queue == 'subject') {
    // console.log('SERVER: ' + queue + " received task");
    // allSpidersRun();
  }
});


var scws_options = {
  cmd: configs.scws.cmd,
  dict: configs.scws.dict,
  top: configs.scws.top
}

var segment = SCWSSegment.getSegment(scws_options);

var str = fs.readFileSync('/opt/stockradar/subject/data/article.txt', 'utf8');

var task = {
  text: str,

};

var table_map = configs.table_map;
/**
 * 对task进行准备工作
 *
 * @param task
 * @param callback
 */
var prepareTask = function(task, callback) {
    if (configs.debug) {
      console.log('----------> prepareTask <-----------');
    }
    if (task.original_task.retry >= 10) {
      var error = {
        error: 'TASK_RETRY_TIMES_LIMITED',
        msg: 'try to deal with the task more than 10 times'
      };
      callback(error, task);
    } else {
      var key = task.hostname + ':' + task.port + ':' + task.database;
      // if(table_map[task.table] != undefined) {
      //   task.table = table_map[task.table];
      // }
      var db = databases[key];
      console.log(key);
      if (db != undefined) {
        task['mysql'] = db;
        task['logger'] = _logger;
        task['debug'] = configs.debug;
        task['segment'] = segment;
        task['keyWords'] = keyWords;
        callback(null, task);
      } else {
        var error = {
          error: 'TASK_DB_NOT_FOUND',
          msg: 'cant not find the database configs included by task URI'
        };
        callback(error, task);
      }
    }
  };

var getCallback = function(info) {
    return function(err, ret) {
      if (err == null) {
        // 所有步骤完成,任务完成
        console.log(utils.getLocaleISOString() + ' task-finished : ' + info.original_task.uri);
        devent.emit('task-finished', info.original_task);
        // 如果页面内容被保存到服务器,将新任务加入到队列
        if (info.save2DBOK && info.new_task_id > 0) {
          var new_task = utils.buildTaskURI({
            protocol: info.protocol,
            hostname: info.hostname,
            port: info.port,
            database: info.database,
            table: 'article_subject',
            id: info.new_task_id
          });
          console.log(utils.getLocaleISOString() + ' NEW_TASK: ' + new_task);
          queue4Similar.enqueue(new_task);
        }
      } else if (err.error == 'TASK_RETRY_TIMES_LIMITED') {
        console.log(utils.getLocaleISOString() + ' 任务尝试次数太多,通知队列任务完成,不在继续尝试' + info.original_task.uri);
        devent.emit('task-finished', info.original_task);
      } else if (err.error == 'TASK_DB_NOT_FOUND') {
        console.log(utils.getLocaleISOString() + ' TASK_DB_NOT_FOUND: ' + info.original_task.uri);
        devent.emit('task-finished', info.original_task);
      } else if (err.error == 'TASK_MICRO_BLOG_NOT_FOUND') {
        console.log(utils.getLocaleISOString() + ' TASK_MICRO_BLOG_NOT_FOUND: ' + info.original_task.uri);
        devent.emit('task-finished', info.original_task);
      } else if (err.error == 'TASK_ARTICLE_CONTENT_NOT_FOUND') {
        console.log(utils.getLocaleISOString() + ' TASK_ARTICLE_CONTENT_NOT_FOUND: ' + info.original_task.uri);
        devent.emit('task-finished', info.original_task);
      } else if (err.error == 'calculateKeyWordsWeight_ERROR') {
        console.log(utils.getLocaleISOString() + ' calculateKeyWordsWeight_ERROR: ' + info.original_task.uri);
      } else if (err.error == 'CONTENT_INCLUDE_MORE_THAN_ONE_BLOCK') {
        console.log(utils.getLocaleISOString() + ' CONTENT_INCLUDE_MORE_THAN_ONE_BLOCK: ' + info.original_task.uri);
        devent.emit('task-finished', info.original_task);
      } else if (err.error == 'CONTENT_BLOCK_NOT_COVER_STOCKS') {
        console.log(utils.getLocaleISOString() + ' CONTENT_BLOCK_NOT_COVER_STOCKS: ' + info.original_task.uri);
        devent.emit('task-finished', info.original_task);
      } else if (err.error == 'CONTENT_INCLUDE_MORE_THEN_TEN_STOCKS') {
        console.log(utils.getLocaleISOString() + ' CONTENT_INCLUDE_MORE_THEN_TEN_STOCKS: ' + info.original_task.uri);
        devent.emit('task-finished', info.original_task);
      } else if (err.error == 'CONTENT_STOCKS_NO_SAME_BLOCK') {
        console.log(utils.getLocaleISOString() + ' CONTENT_STOCKS_NO_SAME_BLOCK: ' + info.original_task.uri);
        devent.emit('task-finished', info.original_task);
      } else if (err.error == 'CONTENT_SINGLE_STOCK_WEIGHT_LESS_60') {
        console.log(utils.getLocaleISOString() + ' CONTENT_STOCKS_NO_SAME_BLOCK: ' + info.original_task.uri);
        devent.emit('task-finished', info.original_task);
      } else {
        console.log(err);
      }
    };
  };

/**
 * 从队列中获取新任务,取到新任务将其压入workFlow队列
 */
var getNewTask = function() {
    if (configs.debug) {
      console.log('----------> getNewTask <-----------');
    }
    var time_stamp = utils.getTimestamp();

    queue4Subject.dequeue(function(error, task) {
      if (error != 'empty' && task != undefined) {
        var time = utils.getTimestamp();
        var task_obj = utils.parseTaskURI(task, time);
        workFlow.push(task_obj);
      } else {
        // console.log('task queue is empty');
      }
    });

  };

var isKeyWordsLoaded = false;
var keyWords = null;
var mysql = databases['127.0.0.1:3306:weibo'];
var doLoadKeyWords = function(cb) {
    var async = require('async');
    async.series({
      stocks: function(callback) {
        mysql.getStockNameAndStockCode(function(result) {
          var stocks = {};
          var stock_name = '';
          for (var i = 0, length = result.length; i < length; i++) {
            stock_name = result[i].stockname.replace(/[\s]/g, '');
            stocks[stock_name] = result[i].stockcode.toLowerCase();
          }
          callback(null, stocks);
        });
      },
      blocks: function(callback) {
        mysql.getBlockNameAndBlockid(function(result) {
          var blocks = {};
          var block_name = '';
          for (var i = 0, length = result.length; i < length; i++) {
            block_name = result[i].block_name.replace(/[\s]/g, '');
            blocks[block_name] = result[i].id;
          }
          callback(null, blocks);
        });
      },
      mapping: function(callback) {
        mysql.getBlockAndStock(function(result) {
          var mappings = {};
          var stock_code = '';
          for (var i = 0, length = result.length; i < length; i++) {
            stock_code = result[i].stock_code.toLowerCase();
            mappings[stock_code] = result[i].block_id;
          }
          callback(null, mappings);
        });
      }
    }, function(err, results) {
      cb(results)
    });
  }

doLoadKeyWords(function(result) {
  isKeyWordsLoaded = true;
  keyWords = result;
  //console.log(keyWords);
});

var worker = Worker.getWorker();
var workFlow = new WorkFlow([prepareTask, worker.getTaskDetailFromDB, worker.getTaskDetailFromDB2, worker.segmentTitle, worker.segmentContent, worker.getArticleSubject, worker.save2Database], getCallback, getNewTask, 1);
setInterval(function() {
  var time_stamp = utils.getTimestamp();
  if (isKeyWordsLoaded && workFlow.getQueueLength() < 1) {
    for (var i = 0; i < 2 - workFlow.getQueueLength(); i++) {
      getNewTask();
    }
  }
}, configs.check_interval);

console.log('Server Started ' + utils.getLocaleISOString());
