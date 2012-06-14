var _ = require('underscore');
var utils = require('./utils');

var Worker = function() {

};

Worker.prototype.getTaskDetailFromDB = function(task, callback) {
  if (task.debug) {
    console.log('----------> getTaskDetailFromDB <-----------');
  }
  task.mysql.get_microblog_content(task.table, task.id, function(result) {
    if (result.length == 0 || result[0].article_id == '') {
      var error = {
        error: 'TASK_MICRO_BLOG_NOT_FOUND',
        msg: ' can not read micro_blog info by id ' + task.id
      };
      callback(error, task);
    } else {
      task['microBlogInfo'] = result[0];
      callback(null, task);
    }
  });
};

Worker.prototype.getTaskDetailFromDB2 = function(task, callback) {
  if (task.debug) {
    console.log('----------> getTaskDetailFromDB2 <-----------');
  }
  task.mysql.get_article_content(task.microBlogInfo.article_id, function(result) {
    if (result.length == 0 || result[0].url == '' || result[0].title == '' || result[0].content == '') {
      var error = {
        error: 'TASK_ARTICLE_CONTENT_NOT_FOUND',
        msg: ' can not read article_content by id ' + task.microBlogInfo.id
      };
      callback(error, task);
    } else {
      task['articleInfo'] = result[0];
      callback(null, task);
    }
  });
};

var filtrateSegmentResult = function(data) {
  var result = [];
  var len = data.length;
  for (var i = 0; i < len; i++) {
    if ('BL' == data[i].attr || 'ST' == data[i].attr) {
      result.push(data[i]);
    }
  }
  return result;
}

Worker.prototype.segmentTitle = function(task, callback) {
  if (task.debug) {
    console.log('----------> segmentTitle article_id: ' + task.articleInfo.id + '<-----------');
  }
  var segmentTask = {
    text: task.articleInfo.title
  };
  task.segment.doSegment(segmentTask, function(result) {
    task['title_keywords'] = filtrateSegmentResult(result);
    callback(null, task);
  });
};

Worker.prototype.segmentContent = function(task, callback) {
  if (task.debug) {
    console.log('----------> segmentContent article_id: ' + task.articleInfo.id + '<-----------');
  }
  var segmentTask = {
    text: task.articleInfo.content
  };
  task.segment.doSegment(segmentTask, function(result) {
    task['content_keywords'] = filtrateSegmentResult(result);
    callback(null, task);
  });
};

var calculateKeyWordsWeight = function(segmentWords, keyWords) {
  // console.log(segmentWords);
  // console.log(keyWords);
  var stocks = [];
  var blocks = [];
  var words = [];
  var stocks_times = 0;
  var blocks_times = 0;
  //将板块和股票分离
  for (var i = 0, len = segmentWords.length; i < len; i++) {
    var word = segmentWords[i].word;
    if (segmentWords[i].attr == 'ST') {
      if (keyWords.stocks[word] != undefined) {
        var stock_code = keyWords.stocks[word];
        var block_id = keyWords.mapping[stock_code] == undefined ? 0: keyWords.mapping[stock_code];
        var obj = {
          code: stock_code,
          times: segmentWords[i].times,
          attr: segmentWords[i].attr,
          name: word,
          block_id: block_id
        }
        stocks_times += segmentWords[i].times;
        words.push(obj);
        stocks.push(obj);
      }
    }
    if (segmentWords[i].attr == 'BL') {
      if (keyWords.blocks[word] != undefined) {
        var obj = {
          code: keyWords.blocks[word],
          times: segmentWords[i].times,
          attr: segmentWords[i].attr,
          name: word
        }
        blocks_times += segmentWords[i].times;
        words.push(obj);
        blocks.push(obj);
      }
    }
  }
  var result = {
    stocks: stocks,
    blocks: blocks,
    words: words,
    stock_times: stocks_times,
    block_times: blocks_times,
    total_times: stocks_times + blocks_times
  };
  return result;
}

var addTimes4BlockTimes = function(collection, sub_key, element) {
  var hit = false;
  for (var i = 0; i < collection.length; i++) {
    if (collection[i][sub_key] == element[sub_key]) {
      collection[i].times += element.times;
      hit = true;
      break;
    }
  }
  if (false == hit) {
    collection.push(element);
  }
  return collection;
}

var getSameBlock4StockList = function(stocks, total_times, percent) {
  var block_times = [];
  for (var i = 0, len = stocks.length; i < len; i++) {
    var block_id = stocks[i].block_id;
    if (block_id != 0) {
      var element = {
        block_id: block_id,
        times: stocks[i].times
      };
      block_times = addTimes4BlockTimes(block_times, 'block_id', element);
    }
  }
  var max_times_block = _.max(block_times, function(ele) {
    return ele.times
  });
  if (max_times_block.times / total_times >= percent) {
    return max_times_block.block_id;
  } else {
    return 0;
  }
};

//关键词过滤,去掉银行板块和需要忽略的证券关键词
var keyWordFilter = function(words) {
  var result = [];
  for (var i = 0, len = words.length; i < len; i++) {
    if ((words[i].attr != 'BL' && words[i].word != '银行') && words[i].attr != 'IG' && words[i].attr != 'GG') {
      result.push(words[i]);
    }
  }
  return result;
};

var isTitleIncludeKeyWordsOfTypeGG = function(words) {
  var result = [];
  for (var i = 0, len = words.length; i < len; i++) {
    if (words[i].attr != 'GG') {
      result.push(words[i]);
    }
  }
  return result;
};

var stockNameFilter = function(stocks) {
  var result = [];
  for (var i = 0, len = stocks.length; i < len; i++) {
    var isFind = false;
    for (var j = 0, result_len = result.length; j < result_len; j++) {
      if (result[j].code == stocks[i].code) {
        result[j].times += stocks[i].times;
        isFind = true;
      }
    }
    if (!isFind) {
      result.push(stocks[i]);
    }
  }
  result = _.sortBy(result, function(stock) {
    return 0 - stock.times
  });
  return result;
};

var debugFileLog = function(task, msg) {
  task.logger.info('-------------------------------------');
  task.logger.info(msg);
  task.logger.info(task.articleInfo.url);
  task.logger.info('标题关键字:' + JSON.stringify(task.title_keywords));
  task.logger.info('正文关键字' + JSON.stringify(task.content_keywords));
};

Worker.prototype.getArticleSubject = function(task, callback) {
  gg_keywords = isTitleIncludeKeyWordsOfTypeGG(task.title_keywords);
  task.title_keywords = keyWordFilter(task.title_keywords);
  task.content_keywords = keyWordFilter(task.content_keywords);
  if (task.debug) {
    console.log('----------> getArticleSubject article_id: ' + task.articleInfo.id + '<-----------');
    console.log(task.articleInfo.url);
    console.log(task.articleInfo.stock_code);
    console.log('title key word:')
    console.log(task.title_keywords);
    console.log('content key word:')
    console.log(task.content_keywords);
  }
  //无法从文章中提取关键词
  if (task.content_keywords.length == 0 && task.title_keywords.length == 0) {
    var error = {
      error: 'ARTICLE_NO_KEY_WORD',
      msg: 'can not segment key words from article'
    };

    debugFileLog(task, '分词错误,无法取得关键词,舍弃');

    callback(error, task);
  } else if (task.title_keywords.length > 0) { //标题中含有板块名称或者股票名称
    var temp = calculateKeyWordsWeight(task.title_keywords, task.keyWords);
    console.log('calculateKeyWordsWeight');
    console.log(temp);
    var stock_key_word_length = temp.stocks.length;
    var block_key_word_length = temp.blocks.length;
    var words_length = temp.words.length;
    if (words_length == 1) { //只有一个关键字
      if (temp.words.length == 0) {
        var error = {
          error: 'calculateKeyWordsWeight_ERROR',
          msg: 'calculateKeyWordsWeight_ERROR'
        };

        console.log('标题中含有多个关键字,ERROR');
        console.log(temp.words);
        debugFileLog(task, '标题中命中一个关键词,但是过滤后关键词数位0,舍弃');

        callback(error, task);
      } else {
        console.log('title include one word: ' + temp.words[0].name);
        task['subject'] = {
          post: temp.words[0]
        };
        callback(null, task);
      }
    } else { //多个关键字，取出现次数多的
      if (temp.words.length == 0) {
        var error = {
          error: 'calculateKeyWordsWeight_ERROR',
          msg: 'calculateKeyWordsWeight_ERROR'
        };
        console.log('标题中含有多个关键字,ERROR');
        console.log(temp.words);

        debugFileLog(task, '标题中命中多个关键词,但是过滤后关键词数位0,舍弃');

        callback(error, task);
      } else {
        console.log('title include more than one words: ' + temp.words[0].name);
        task['subject'] = {
          post: temp.words[0]
        };
        callback(null, task);
      }
    }
  } else { //标题中不含关键字
    var temp = calculateKeyWordsWeight(task.content_keywords, task.keyWords);
    temp.stocks = stockNameFilter(temp.stocks);
    //正文中命中板块关键字
    if (temp.blocks.length > 0) {
      console.log('有板块命中');
      //命中多个板块
      if (temp.blocks.length > 1) {
        //舍弃
        console.log('板块超过1个舍弃');
        var error = {
          error: 'CONTENT_INCLUDE_MORE_THAN_ONE_BLOCK',
          msg: '文章正文提到超过1个板块,舍弃'
        };

        debugFileLog(task, '文章正文提到超过1个板块,舍弃');

        callback(error, task);
      } else { //只有一个板块命中
        console.log('只有一个板块命中');
        var max_time_block_id = getSameBlock4StockList(temp.stocks, temp.stock_times, 0.8);
        if (max_time_block_id == temp.blocks[0].code) {
          task['subject'] = {
            post: temp.blocks[0]
          };
          callback(null, task);
        } else {
          var error = {
            error: 'CONTENT_BLOCK_NOT_COVER_STOCKS',
            msg: '文章正文提到的板块不能涵盖大部分股票,舍弃'
          };

          debugFileLog(task, '文章正文提到的板块不能涵盖大部分股票,舍弃');

          callback(error, task);
        }
      }
    } else {
      console.log('无板块命中');
      if (temp.stocks.length > 10) { //股票出现次数超过10个
        //TODO:需要判断是否是公告类文章
        if (gg_keywords.length > 0) {
          var error = {
            error: 'CONTENT_INCLUDE_MORE_THEN_TEN_STOCKS',
            msg: '超过10只股票,疑似公告类文章,舍弃'
          };
          debugFileLog(task, '超过10之股票,疑似公告类文章,舍弃' + JSON.stringify(gg_keywords));
          callback(error, task);
        } else if (temp.stocks.length * 4 / task.articleInfo.content.length > 0.95) {
          var error = {
            error: 'CONTENT_INCLUDE_MORE_THEN_TEN_STOCKS',
            msg: '超过10只股票,股票名称占文章比例太高,舍弃'
          };

          debugFileLog(task, '超过10之股票,股票名称占文章比例太高,舍弃');

          callback(error, task);
        } else {
          console.log('超过10只股票, 转发到A股雷达');
          task['subject'] = {
            post: {
              code: 'a_stock',
              attr: 'AS'
            },
            repost: []
          };
          for (var n = 0, len = temp.stocks.length; n < len; n++) {
            var temp_stock_obj = {};
            temp_stock_obj = {
              code: temp.stocks[n].code,
              attr: 'ST'
            }

            task['subject'].repost.push(temp_stock_obj);
          }
          callback(null, task);
        }
      } else if (temp.stocks.length > 3) {
        var max_time_block_id = getSameBlock4StockList(temp.stocks, temp.stock_times, 0.8);
        if (max_time_block_id > 0) {
          console.log('有共同的板块 ID: ' + max_time_block_id);
          var block_temp_obj = {
            code: max_time_block_id,
            attr: 'BL'
          };
          task['subject'] = {
            post: block_temp_obj
          };
          callback(null, task);
        } else {
          console.log('无共同板块 keywords > 3 转发到A股雷达');
          task['subject'] = {
            post: {
              code: 'a_stock',
              attr: 'AS'
            },
            repost: []
          }
          for (var n = 0, len = temp.stocks.length; n < len; n++) {
            var temp_stock_obj = {};
            temp_stock_obj = {
              code: temp.stocks[n].code,
              attr: 'ST'
            }
            task['subject'].repost.push(temp_stock_obj);
          }
          callback(null, task);
        }
      } else {
        if (temp.stocks[0].times / temp.stock_times > 0.6) {
          console.log('一只股票出现次数 超过 60%');
          task['subject'] = {
            post: temp.stocks[0]
          };
          callback(null, task);
        } else {
          console.log('无单只股票出现次数 超过 60% 发送到第一支股票上,其他股票转发');
          task['subject'] = {
            post: {},
            repost: []
          }
          for (var n = 0, len = temp.stocks.length; n < len; n++) {
            var temp_stock_obj = {};

            temp_stock_obj = {
              code: temp.stocks[n].code,
              attr: 'ST'
            }
            if (n == 0) {
              task['subject'].post = temp_stock_obj;
            } else {
              task['subject'].repost.push(temp_stock_obj);
            }
          }
          callback(error, task);
        }
      }
    }
  }
};

Worker.prototype.save2Database = function(task, callback) {
  //TODO 将主题保存到数据库
  if (task.debug) {
    console.log('#----------> save2Database article_id: ' + task.articleInfo.id + '<-----------');
  }
  //function(micro_blog_id, article_id, stock_code, block_id, send_type, in_time, cb) {
  var micro_blog_id = task.id;
  var article_id = task.microBlogInfo.article_id;
  var stock_code = task.subject.post.attr == 'ST' ? task.subject.post.code: '';
  if (task.subject.post.attr == 'AS') {
    stock_code = 'a_stock';
  }
  var block_id = task.subject.post.attr == 'BL' ? task.subject.post.code: 0;
  var send_type = 'post';
  var in_time = utils.getTimestamp();

  task.mysql.save2ArticleSubject(micro_blog_id, article_id, stock_code, block_id, send_type, in_time, function(insert_id) {
    console.log(insert_id)
    if (insert_id > 0) {
      task['save2DBOK'] = true;
      task['new_task_id'] = insert_id;
      callback(null, task);
      if (task.subject.repost != undefined) {
        var temp_stock_list = [];
        for (var i = 0; i < task.subject.repost.length; i++) {

          stock_code = task.subject.repost[i].code;
          block_id = 0;
          send_type = 'repost';
          if(temp_stock_list.indexOf(stock_code) == -1) {
            temp_stock_list.push(stock_code);
            task.mysql.save2ArticleSubject(micro_blog_id, article_id, stock_code, block_id, send_type, in_time, function(insert_id) {
              console.log('add repost to article_subject table');
            });  
          }
        }
      }
    } else {
      console.log('无单只股票出现次数 超过 60%  舍弃');
      var error = {
        error: 'SAVE_2_DB_ERROR',
        msg: '保存到数据库错误'
      };
      callback(error, task);
    }
  });

};

exports.Worker = Worker;
exports.getWorker = function() {
  return new Worker();
};

