/**
 * Created by shixi_dongxue3 on 17/12/06.
 */

//ajax中error的处理
function turnToErrorPage(readyState, status, errorThrown) {
    localStorage.setItem("readyState", readyState);
    localStorage.setItem("status", status);
    localStorage.setItem("errorThrown", errorThrown);
    location.href = "/404.html";
}

//将时间戳转换成时间
Date.prototype.format = function (format) {
    var date = {
        "M+": this.getMonth() + 1,
        "d+": this.getDate(),
        "h+": this.getHours(),
        "m+": this.getMinutes(),
        "s+": this.getSeconds(),
        "q+": Math.floor((this.getMonth() + 3) / 3),
        "S+": this.getMilliseconds()
    };
    if (/(y+)/i.test(format)) {
        format = format.replace(RegExp.$1, (this.getFullYear() + '').substr(4 - RegExp.$1.length));
    }
    for (var k in date) {
        if (new RegExp("(" + k + ")").test(format)) {
            format = format.replace(RegExp.$1, RegExp.$1.length == 1 ? date[k] : ("00" + date[k]).substr(("" + date[k]).length));
        }
    }
    return format;
}

//返回
function closeLayer() {
    var index = parent.layer.getFrameIndex(window.name); //获取当前弹出层的层级
    parent.layer.close(index); //关闭弹出层
}

//添加产品线下拉菜单中的内容
function listProduct() {
    /*添加产品线下拉菜单中的内容*/
    $(document).ready(function () {
        $.ajax({
            type: 'GET',
            url: "/listProduct",
            async: true,
            success: function (data) {
                for (i in data.productList) {
                    var option = '<option value="' + data.productList[i] + '">' + data.productList[i] + '</option>';
                    $('#product').append(option);
                }
            },
            error: function (XMLHttpRequest, textStatus, errorThrown) {
                turnToErrorPage(XMLHttpRequest.readyState, XMLHttpRequest.status, errorThrown);
            }
        });
    });
}

//获得集群的下拉菜单
function listCluster() {
    $(document).ready(function () {
        $.ajax({
            type: 'GET',
            url: "/listCluster",
            async: true,
            success: function (data) {
                for (i in data.clusterList) {
                    var option = '<option value="' + data.clusterList[i] + '">' + data.clusterList[i] + '</option>';
                    $('#cluster_name').append(option);
                }
            },
            error: function (XMLHttpRequest, textStatus, errorThrown) {
                turnToErrorPage(XMLHttpRequest.readyState, XMLHttpRequest.status, errorThrown);
            }
        })
    })
}

//添加bussiness下拉菜单中的内容
function listBusiness() {
    /*添加产品线下拉菜单中的内容*/
    $(document).ready(function () {
        $.ajax({
            type: 'GET',
            url: "/listBusiness",
            async: true,
            success: function (data) {
                for (i in data.businessList) {
                    var option = '<option value="' + data.businessList[i] + '">' + data.businessList[i] + '</option>';
                    $('#business').append(option);
                }
            },
            error: function (XMLHttpRequest, textStatus, errorThrown) {
                turnToErrorPage(XMLHttpRequest.readyState, XMLHttpRequest.status, errorThrown);
            }
        });
    });
}

//添加多选框效果
function addCheckboxEffect() {
    /*多选框效果*/
    $(".metrics-list dd input:checkbox").click(function () {
        var l = $(this).parent().parent().find("input:checked").length;
        var l2 = $(this).parents(".metrics-list").find(".metrics-list dd").find("input:checked").length;
        if ($(this).prop("checked")) {
            $(this).closest("dl").find("dt input:checkbox").prop("checked", true);
            $(this).parents(".metrics-list").find("dt").first().find("input:checkbox").prop("checked", true);
        }
    });
}

//策略中英文名称对应
function getMetricsStrategyName(strategyName) {
    switch (strategyName) {
        case "simpleThreshold":
            return "简单阈值";
            break;
        case "mean":
            return "均值漂移";
            break;
        case "cyclePer":
            return "同比(百分比)";
            break;
        case "cycle":
            return "同比";
            break;
        case "annulus":
            return "环比";
            break;
        case "deviation":
            return "平均值和标准差";
            break;
        case "forecast":
            return "时间序列预测";
            break;
    }
}

//获取策略下的参数
function getStatistics(strategyName) {
    switch (strategyName) {
        case "simpleThreshold":
            return [['min', '阈值下限', ''], ['max', '阈值上限', '']];
            break;
        case "mean":
            return [['threshold', '阈值', ''], ['windows', '窗口宽度', '']];
            break;
        case "cyclePer":
            return [['t', '周期', '日'], ['per', '阈值(百分比)', ''], ['windows', '窗口宽度', '']];
            break;
        case "cycle":
            return [['t', '周期', '日'], ['threshold', '阈值', ''], ['windows', '窗口宽度', '']];
            break;
        case "annulus":
            return [['threshold', '阈值', '']];
            break;
        case "deviation":
            return [['t', '周期', '日'], ['period', '时间段长度', '周期的多少倍'], ['a', '阈值系数', '']];
            break;
        case "forecast":
            return [['t', '周期', '日'], ['trainPeriod', '训练时长', '周期的多少倍'], ['predictPeriod', '预测时长', '周期的多少倍']];
            break;
    }
}

//加载指标监控策略内容
function sectionContent(metricsName) {
    var content = '';
    var strategyName = ['simpleThreshold', 'mean', 'cyclePer', 'cycle', 'annulus', 'deviation', 'forecast'];
    for (var i = 0; i < strategyName.length; i++) {
        content += '<dl class="cl metrics-list2"><dt><label><input type="checkbox" name="' + metricsName
            + '" value="' + strategyName[i] + '" onchange="javascript:loadInput(this)"/>' + getMetricsStrategyName(strategyName[i])
            + '</label></dt><dd>';
        for (var j = 0; j < getStatistics(strategyName[i]).length; j++) {
            content += '<label><input type="hidden" name="' + metricsName + '_' + strategyName[i] + '" value="' + getStatistics(strategyName[i])[j][0]
                + '"/>' + getStatistics(strategyName[i])[j][1] + '<input type="number" class="input-text" disabled="true" id="' + metricsName + '_' + strategyName[i] + '_' + getStatistics(strategyName[i])[j][0]
                + '" placeholder="' + getStatistics(strategyName[i])[j][2] + '"/></label>';
        }
        content += '</dd></dl>';
    }

    return content;
}