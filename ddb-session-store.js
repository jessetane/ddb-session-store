/*
 * A node-dynamodb backed session store for use with connect.session
 * 
 * 
 */


module.exports = function (connect) {
  
  var Store = connect.session.Store;

  function DDBSessionStore (opts) {
    opts || (opts = {});
    this.table = opts.table || "sessions";
    this.pk = opts.pk || "id";
    this.ddb = opts.client;
    
    if (opts.reapInterval > 0) {
      var t = this;
      setInterval(function () {
        var fopts = { filter : { expires: { le: +new Date }}};
        t.ddb.scan(t.table, fopts, function (err, res) {
          if (!err && res.count) {
            var batch = {};
            var items = batch[t.table] = [];
            for (var item in res.items) items.push(item[t.pk]);
            t.ddb.batchWriteItem(null, batch, function (err, res) {});
          }
        });
      }, opts.reapInterval);
    }
  };

  DDBSessionStore.prototype.__proto__ = Store.prototype;

  DDBSessionStore.prototype.get = function (sid, fn){
    var t = this;
    this.ddb.getItem(this.table, sid, null, {}, function (err, res, cap) {
      if (err) {
        fn(err);
      } else if (res) {
        res = JSON.parse(res.data);
        fn(null, res);
      } else {
        fn();
      }
    });
  };

  DDBSessionStore.prototype.set = function (sid, sess, fn){
    var s = { data: JSON.stringify(sess) };
    s[this.pk] = sid;
    if (!sess.cookie.expires) {
      var d = new Date();
      s.expires = d.setFullYear(99999);
    } else {
      s.expires = sess.cookie.expires.getTime();
    }
    this.ddb.putItem(this.table, s, {}, function (err, res, cap) {
      fn && fn(err || res);
    });
  };

  DDBSessionStore.prototype.destroy = function (sid, fn){
    this.ddb.deleteItem(this.table, sid, null, {}, function (err, res, cap) {
      fn && fn(err || res);
    });
  };

  return DDBSessionStore;
};