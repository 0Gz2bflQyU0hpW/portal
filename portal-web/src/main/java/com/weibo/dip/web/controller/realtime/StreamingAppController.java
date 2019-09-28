package com.weibo.dip.web.controller.realtime;

import com.weibo.dip.web.model.AnalyzeJson;
import com.weibo.dip.web.model.realtime.StreamingInfo;
import com.weibo.dip.web.service.StreamingAppService;

import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
@RequestMapping("/realtime/application")
public class StreamingAppController {

  private static final Logger LOGGER = LoggerFactory.getLogger(StreamingAppController.class);

  private StreamingAppService streamingAppService;

  @Autowired
  public StreamingAppController(StreamingAppService streamingAppService) {
    this.streamingAppService = streamingAppService;
  }

  /**
   * 启动应用.
   *
   * @param appname appname
   * @return success or not
   */
  @RequestMapping(value = "/start", method = RequestMethod.POST)
  @ResponseBody
  public int start(
      @RequestParam("appname") String appname, @RequestParam("creator") String creator) {
    return streamingAppService.start(appname, creator);
  }

  /**
   * 停止应用.
   *
   * @param appname appname
   * @return success or not
   */
  @RequestMapping(value = "/stop", method = RequestMethod.POST)
  @ResponseBody
  public int stop(@RequestParam("appname") String appname) {
    return streamingAppService.stop(appname);
  }

  /**
   * 重启应用.
   *
   * @param appname appname
   * @return success or not
   */
  @RequestMapping(value = "/restart", method = RequestMethod.POST)
  @ResponseBody
  public int restart(@RequestParam("appname") String appname) {
    return streamingAppService.restart(appname);
  }

  /**
   * 使用datatable列出所有的app信息.
   *
   * @param data data
   * @return string
   */
  @RequestMapping(value = "/datatable", method = RequestMethod.POST)
  @ResponseBody
  public String list(@RequestParam("data") String data) {
    AnalyzeJson json = new AnalyzeJson(data);
    List<StreamingInfo> list = streamingAppService.list();
    return json.getResponseString(list);
  }

  /**
   * 保存应用信息.
   *
   * @param streamingInfo info
   */
  @RequestMapping(value = "/insert", method = RequestMethod.POST)
  @ResponseBody
  public int insert(@RequestBody StreamingInfo streamingInfo) {
    return streamingAppService.insert(streamingInfo);
  }

  /**
   * 根据id 删除指定app.
   *
   * @param appname name
   * @return result
   */
  @RequestMapping(value = "/delete", method = RequestMethod.POST)
  @ResponseBody
  public int delete(@RequestParam("appname") String appname) {
    return streamingAppService.delete(appname);
  }
  /**
   * 更新应用信息.
   *
   * @param appInfo i
   * @return i
   */
  @RequestMapping(value = "/update", method = RequestMethod.POST)
  @ResponseBody
  public int update(@RequestBody StreamingInfo appInfo) {
    return streamingAppService.update(appInfo);
  }

  /**
   * find by name.
   *
   * @param appname name
   */
  @RequestMapping(value = "/findbyname", method = RequestMethod.POST)
  @ResponseBody
  public StreamingInfo findByName(@RequestParam("appname") String appname) {
    return streamingAppService.findByName(appname);
  }

  /**
   * is exit.
   *
   * @param appname name
   */
  @RequestMapping(value = "/isexist", method = RequestMethod.POST)
  @ResponseBody
  public boolean isExit(@RequestParam("appname") String appname) {
    return streamingAppService.isExit(appname);
  }
}
