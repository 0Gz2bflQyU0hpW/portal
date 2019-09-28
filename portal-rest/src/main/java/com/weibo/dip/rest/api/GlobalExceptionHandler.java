package com.weibo.dip.rest.api;

import com.weibo.dip.rest.bean.Result;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;

/**
 * Created by yurun on 17/10/25.
 */
@ControllerAdvice
public class GlobalExceptionHandler {

    @ResponseStatus(value=HttpStatus.BAD_REQUEST)
    @ExceptionHandler(Throwable.class)
    @ResponseBody
    public Result defaultExceptionHandler(Throwable t) {
        return new Result(HttpStatus.BAD_REQUEST, t.getMessage());
    }

}
