package com.thinkbox.reactive.controller;

import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.thinkbox.reactive.service.ParseService;

import lombok.extern.slf4j.Slf4j;

@RestController
@RequestMapping("")
@Slf4j
public class MainController {

	@Autowired
	private ParseService parseService;

	private static final String APPLICATION_JSON_TYPE = "application/json";

	@PostMapping(path = "flux2", consumes = APPLICATION_JSON_TYPE, produces = APPLICATION_JSON_TYPE)
	public ResponseEntity<Object> processFlux2(@RequestBody Map<String, Object> map) throws Exception {
		log.info("Received map: {}", map.toString());

		String filename = map.get("filename").toString();

		parseService.processFlux2(filename);
		return ResponseEntity.ok(map);
	}

	@PostMapping(path = "flux", consumes = APPLICATION_JSON_TYPE, produces = APPLICATION_JSON_TYPE)
	public ResponseEntity<Object> processFlux(@RequestBody Map<String, Object> map) throws Exception {
		log.info("Received map: {}", map.toString());

		String filename = map.get("filename").toString();

		parseService.processFlux(filename);
		return ResponseEntity.ok(map);
	}

	@PostMapping(path = "list", consumes = APPLICATION_JSON_TYPE, produces = APPLICATION_JSON_TYPE)
	public ResponseEntity<Object> processList(@RequestBody Map<String, Object> map) throws Exception {
		log.info("Received map: {}", map.toString());

		String filename = map.get("filename").toString();

		parseService.processList(filename);
		return ResponseEntity.ok(map);
	}
}