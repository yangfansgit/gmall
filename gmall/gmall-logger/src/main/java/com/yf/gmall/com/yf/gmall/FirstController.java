package com.yf.gmall.com.yf.gmall;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;

// @RestController 表示返回结果全是字符串，不需要在每个方法加@ResponseBody
@Controller
public class FirstController {

    // 请求的方法名称
    @RequestMapping("/first")
    // 返回值是字符串
    @ResponseBody
    public String test() throws IOException {

        return "this is first controller";

    }
}
