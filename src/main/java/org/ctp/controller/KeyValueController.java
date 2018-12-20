package org.ctp.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class KeyValueController {

    @RequestMapping("/index")
    public String index() {
        return "Hello World";
    }
}
