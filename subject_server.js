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
}

// 将pid写入文件，以便翻滚日志时读取
fs.writeFileSync(__dirname + '/run/server.lock', process.pid.toString(), 'ascii');

// 页面内容队列
var queue4PageContent = queue.getQueue('http://' + configs.queue_server.host + ':' + configs.queue_server.port + '/' + configs.queue_server.queue_path, configs.subject_generate_queue);
// url队列
var queue4Url = queue.getQueue('http://' + configs.queue_server.host + ':' + configs.queue_server.port + '/' + configs.queue_server.queue_path, configs.subject_monitor_queue);

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

var str = fs.readFileSync('/home/jason/workspace/StockRadar/subject/data/article.txt', 'utf8');

var task = {
  text: str,

};

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
      var db = databases[key];
      if (db != undefined) {
        task['mysql'] = db;
        task['logger'] = _logger;
        task['debug'] = configs.debug;
        task['segment'] = segment;
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
            table: 'page_content',
            id: info.new_task_id
          });
          console.log(utils.getLocaleISOString() + ' NEW_TASK: ' + new_task);
          queue4PageContent.enqueue(new_task);
        }
        if (info.pageContentUnchanged) {
          console.log(utils.getLocaleISOString() + ' page content is not changed: ' + info.original_task.uri);
        }
      } else if (err.error == 'TASK_RETRY_TIMES_LIMITED') {
        console.log(utils.getLocaleISOString() + ' 任务尝试次数太多,通知队列任务完成,不在继续尝试' + info.original_task.uri);
        devent.emit('task-finished', info.original_task);
      } else if (err.error == 'TASK_DB_NOT_FOUND') {
        console.log(utils.getLocaleISOString() + ' TASK_DB_NOT_FOUND: ' + info.original_task.uri);
        devent.emit('task-finished', info.original_task);
      } else if (err.error == 'TASK_URL_NOT_FOUND') {
        console.log(utils.getLocaleISOString() + ' TASK_URL_NOT_FOUND: ' + info.original_task.uri);
        devent.emit('task-finished', info.original_task);
      } else if (err.error == 'PAGE_CONTENT_UNCHANGED') {
        console.log(utils.getLocaleISOString() + ' PAGE_CONTENT_UNCHANGED: ' + info.original_task.uri);
        devent.emit('task-finished', info.original_task);
      } else if (err.error == 'FETCH_URL_ERROR') {
        console.log(utils.getLocaleISOString() + ' FETCH_URL_ERROR do nothing:' + info.original_task.uri);
        // devent.emit('task-error', info.original_task);
      } else if (err.error == 'PAGE_CONTENT_SAVE_2_DB_ERROR') {
        devent.emit('task-error', info.original_task);
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

    queue4Url.dequeue(function(error, task) {
      if (error != 'empty' && task != undefined) {
        var time = utils.getTimestamp();
        var task_obj = utils.parseTaskURI(task, time);
        workFlow.push(task_obj);
      } else {
        // console.log('task queue is empty');
      }
    });

  };

var worker = Worker.getWorker();
console.log(worker);
var workFlow = new WorkFlow([prepareTask, worker.getTaskDetailFromDB, worker.segmentTitle, worker.segmentContent, worker.getArticleSubject, worker.save2Database], getCallback, getNewTask, configs.worker_count);

setInterval(function() {
  var time_stamp = utils.getTimestamp();
  if (workFlow.getQueueLength() < 2) {
    for (var i = 0; i < 50 - workFlow.getQueueLength(); i++) {
      getNewTask();
    }
  }
}, configs.check_interval);

console.log('Server Started ' + utils.getLocaleISOString());
