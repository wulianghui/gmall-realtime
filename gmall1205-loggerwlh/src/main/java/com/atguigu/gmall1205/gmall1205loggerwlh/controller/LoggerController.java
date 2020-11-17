package com.atguigu.gmall1205.gmall1205loggerwlh.controller;



import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall1205.common.constant.GmallConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

@RestController  //Controller + Responsebody
public class LoggerController {

    @Autowired
    KafkaTemplate<String,String> kafkaTemplate;

    private static final Logger logger = LoggerFactory.getLogger(LoggerController.class);


    //@RequestMapping(value = "log",method = RequestMethod.POST)
    @PostMapping("/log")
    //@ResponseBody
    public String dolog(@RequestParam("log") String logJson){



        //补时间戳
        JSONObject jsonObject = JSON.parseObject(logJson);
        jsonObject.put("ts",System.currentTimeMillis());


        //落盘和logfile   log4j
        logger.info(jsonObject.toJSONString());

        //发送kafka
        if("startup".equals(jsonObject.getString("type"))){
            kafkaTemplate.send(GmallConstant.KAFKA_TOPIC_STARTUP,jsonObject.toJSONString());
        }else{
            kafkaTemplate.send(GmallConstant.KAFKA_TOPIC_EVENT,jsonObject.toJSONString());
        }



        //System.out.println(logJson);
        return "success";
    }
}
