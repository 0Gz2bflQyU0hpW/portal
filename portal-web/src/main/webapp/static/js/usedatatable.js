/**
 * Created by haisen 18/4/28.
 */

var path = location.pathname;
var patharray = splitPath();
//前半部分的公用路径
var pathname = getFatherpath();

//初始化DataTable,固定 dom元素为 .table-sort  , 必须要指定列的显示，路径自动解析，所以要求路径格式统一。
function createMyDataTable(columns) {
  var datatable = $('.table-sort').DataTable({
    "searching": false,
    "lengthMenu": [10, 30, 50, 70, 100],
    "serverSide": true,
    "retrieve": true,
    "stateSave": true,
    "columns": columns,
    "ajax": {
      url: pathname + 'datatable',
      type: 'POST',
      data: function (aoData) {
        return {"data": JSON.stringify(aoData)};
      }
    }
  });
  return datatable;
}

//添加搜索功能 dom元素为 id ,datatable为要刷新的
function addSearching(id, datatable) {
  $('#' + id).on('click', function () {
    var condition = $('#byField').val();
    var starttime = $('#start').val();
    var endtime = $('#end').val();
    var keyword = $('#keyword').val();
    //修改 要发送的数据
    datatable.settings()[0].ajax.data = function (aoData) {
      var param = {
        "condition": condition,
        "starttime": starttime,
        "endtime": endtime,
        "keyword": keyword
      };
      var d = $.extend({}, aoData, param);
      var data = JSON.stringify(d);
      return {
        "data": data
      };
    };
    datatable.ajax.reload();
  });
}

//有3个操作按钮的页面, 查看、修改和删除
function operation3(datatable, titlename) {
  $('#talbe_list_body').on('click', 'tr>td>a>i', function () {
    var trparent = $(this).parents('tr');
    var trdata = datatable.row(trparent).data();
    var id = trdata.id;
    var index = $(this).parents('a').index();
    if (index == 0) {//show
      var title = titlename + '详情';
      var url = getShowpath(id);
      layer_show(title, url, '800', '600');
    } else if (index == 1) {//update
      var title = '编辑' + titlename;
      var url = getUpdatepath(id);
      layer_show(title, url, '800', '600');
    } else if (index == 2) {//delete
      deleteColumn(id, datatable);
    }
  });
}

//路径解析
function splitPath() {
  var array = path.split("/");
  return array;
}

//得到前半部分路径
function getFatherpath() {
  var fatherpath = "";
  for (var i = 1; i < patharray.length - 1; i++) {
    fatherpath += "/" + patharray[i];
  }
  fatherpath += "/";
  return fatherpath;
}

function getAddpath() {
  var name = patharray[patharray.length - 2];
  name += '-add.html';
  return pathname + name;
}

function getShowpath(id) {
  var name = patharray[patharray.length - 2];
  name += '-show.html?id=' + id;
  return pathname + name;
}

function getUpdatepath(id) {
  var name = patharray[patharray.length - 2];
  name += '-update.html?id=' + id;
  return pathname + name;
}

function getDeletepath() {
  var deletepath = pathname + 'delete';
  return deletepath;
}

//删除一行
function deleteColumn(id, datatable) {
  layer.confirm('确定删除吗？', function (indes) {
    $.ajax({
      type: 'post',
      url: getDeletepath(),
      data: {
        'id': id
      },
      success: function () {
        datatable.draw(true);
        layer.msg('已删除!', {icon: 5, time: 1000});
      },
      error: function (XMLHttpRequest, textStatus, errorThrown) {
        turnToErrorPage(XMLHttpRequest.readyState, XMLHttpRequest.status,
            errorThrown);
      }
    });
  });
}

/*
//添加数据
function addData(title, url) {
    layer_show(title, url, '800', '600');
}*/
