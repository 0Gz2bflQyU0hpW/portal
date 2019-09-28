package com.weibo.dip.rest.api;

import com.weibo.dip.rest.bean.Result;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Created by yurun on 17/10/25.
 */
@RestController
@RequestMapping("/demo")
public class DemoController {

    @RequestMapping(value = "get", method = RequestMethod.GET)
    public Result get(@RequestParam String name, @RequestParam int age) {
        return new Result(HttpStatus.OK, "hello " + name + ", age " + age);
    }

    @RequestMapping(value = "post", method = RequestMethod.POST)
    public Result post(@RequestParam String name, @RequestParam int age) {
        return new Result(HttpStatus.OK, "hello " + name + ", age " + age);
    }

}
