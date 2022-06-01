package com.thinkbox.reactive.service;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.BaseStream;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.thinkbox.reactive.config.MapKeyParameter;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

@Component
@Slf4j
public class ParseService {

	private static final ObjectMapper objectMapper = new ObjectMapper();

	private final static String USER_HOME = "user.home";

	private final static String OUTPUT_DATE_FORMAT = "%1$tY%1$tm%1$td";

	private final static String DATE_FORMAT_STOOP = "yyyyMMdd";

	private final static List<Integer> COLUMN_LIST_STOOP = Arrays.asList(2, 4, 5, 6, 7, 7, 8);

	private final static String MARKET_CANADA = "CA";

	private final static String MARKET_UNITED_STATE = "US";

	private final static String TICKER_SUFFIX_TORONTO_STOCK_EXCHANGE = ".TO";

	private final static String TICKER_SUFFIX_TORONTO_STOCK_VENTURE_EXCHANGE = ".V";

	private final static String STOOQ_TICKER_SUFFIX = ".US";

	private final static String DEFAULT_STRING_VALUE = "-";

	private final static String STRING_COMMA = ",";

	private final static String STRING_SQUARE_OPEN_BRACKET = "[";

	private final static String STRING_SQUARE_CLOSE_BRACKET = "]";

	private final static String STRING_CURLY_BRACKET = "{}";

	@Value("${app.data.directory:-}")
	private String dataDirectory;

	@Autowired
	private MapKeyParameter mapKey;

	@PostConstruct
	public void init() {
		if (dataDirectory != null && dataDirectory.equals(DEFAULT_STRING_VALUE)) {
			dataDirectory = System.getProperty(USER_HOME);
		}
	}

	public void processList(String fileName) throws Exception {

		String inFilePath = System.getProperty(USER_HOME) + "/" + fileName;
		String outFilePath2 = System.getProperty(USER_HOME) + "/" + "json-sync-" + fileName;

		Path path = Paths.get(inFilePath);

		if (!path.toFile().isFile()) {
			log.error("The file " + path + " doesn't exist or is not a text file");
			System.exit(-1);
		} else {
			log.info("The file " + path + " exist");
		}
		System.gc();
		listVersion(inFilePath, outFilePath2);
	}

	public void processFlux(String fileName) throws Exception {

		String inFilePath = System.getProperty(USER_HOME) + "/" + fileName;
		String outFilePath = System.getProperty(USER_HOME) + "/" + "json-" + fileName;

		Path path = Paths.get(inFilePath);

		if (!path.toFile().isFile()) {
			log.error("The file " + path + " doesn't exist or is not a text file");
			System.exit(-1);
		} else {
			log.info("The file " + path + " exist");
		}
		System.gc();
		fluxVersion(inFilePath, outFilePath);
	}

	private void fluxVersion(String inFilePath, String outFilePath) {

		Path inPath = Paths.get(inFilePath);
		Path outPath = Paths.get(outFilePath);
		BufferedWriter bw;
		try {
			bw = Files.newBufferedWriter(outPath, StandardOpenOption.CREATE);
			bw.write(STRING_SQUARE_OPEN_BRACKET);
			Long start = System.currentTimeMillis();

			Flux.using(() -> Files.lines(inPath), Flux::fromStream, BaseStream::close).skip(1)
					.map(s -> parseLine(s.split(","))).subscribeOn(Schedulers.newSingle("json-file"))
					.subscribe(s -> write(bw, s), (e) -> close(bw, start), () -> close(bw, start));

		} catch (IOException e1) {
			e1.printStackTrace();
		}

	}

	public void processFlux2(String fileName) throws Exception {

		String inFilePath = System.getProperty(USER_HOME) + "/" + fileName;
		String outFilePath = System.getProperty(USER_HOME) + "/" + "json-" + fileName;
		String outFilePath2 = System.getProperty(USER_HOME) + "/" + "json-2-" + fileName;

		Path path = Paths.get(inFilePath);

		if (!path.toFile().isFile()) {
			log.error("The file " + path + " doesn't exist or is not a text file");
			System.exit(-1);
		} else {
			log.info("The file " + path + " exist");
		}
		System.gc();
		fluxVersion2(inFilePath, outFilePath, outFilePath2);
	}

	private void fluxVersion2(String inFilePath, String outFilePath, String outFilePath2) {

		Path inPath = Paths.get(inFilePath);
		Path outPath = Paths.get(outFilePath);
		Path outPath2 = Paths.get(outFilePath2);

		try {
			final Long start = System.currentTimeMillis();

			Flux<Map<String, Object>> flux = Flux.using(() -> Files.lines(inPath), Flux::fromStream, BaseStream::close)
					.skip(1).map(s -> parseLine(s.split(","))).subscribeOn(Schedulers.newParallel("file-copy", 2))
					.share();

			BufferedWriter bw = Files.newBufferedWriter(outPath, StandardOpenOption.CREATE);
			bw.write(STRING_SQUARE_OPEN_BRACKET);
			flux.subscribe(s -> write(bw, s), (e) -> close(bw, start), () -> close(bw, start));

			BufferedWriter bw2 = Files.newBufferedWriter(outPath2, StandardOpenOption.CREATE);
			flux.subscribe(s -> write2(bw2, s), (e) -> close2(bw2, start), () -> close2(bw2, start));

		} catch (IOException e1) {
			e1.printStackTrace();
		}

	}

	private void close(BufferedWriter bw, Long start) {
		try {
			bw.write(STRING_CURLY_BRACKET + STRING_SQUARE_CLOSE_BRACKET);
			bw.close();
			log.info("Closed the resource");
			Long end = System.currentTimeMillis();
			log.info("Total time:" + (end - start));

			final Runtime runtime = Runtime.getRuntime();
			System.gc();
			log.info("Memory in use while reading: {}MB\n",
					(runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024));
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	private void write(BufferedWriter bw, Map<String, Object> map) {
		try {
			bw.write(objectMapper.writeValueAsString(map) + STRING_COMMA);
			bw.newLine();
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	private void close2(BufferedWriter bw, Long start) {
		try {
			bw.close();
			log.info("Closed the resource");
			Long end = System.currentTimeMillis();
			log.info("Total time:" + (end - start));

			final Runtime runtime = Runtime.getRuntime();
			System.gc();
			log.info("Memory in use while reading: {}MB\n",
					(runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024));
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	private void write2(BufferedWriter bw, Map<String, Object> map) {
		try {
			bw.write(objectMapper.writeValueAsString(map));
			bw.newLine();
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	private void listVersion(String inFilePath, String outFilePath) throws IOException {
		final Runtime runtime = Runtime.getRuntime();
		Long start = System.currentTimeMillis();

		List<Map<String, Object>> list = parseQuoteFile(inFilePath);

		objectMapper.writeValue(new File(outFilePath), list);

		Long end = System.currentTimeMillis();
		log.info("Total time:" + (end - start));

		System.gc();
		log.info("Memory in use while reading: {}MB\n", (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024));

	}

	private List<Map<String, Object>> parseQuoteFile(final String fullFileName) throws IOException {

		Path inPath = Paths.get(fullFileName);

		List<Map<String, Object>> outputList = Files.lines(inPath).skip(1).map(s -> parseLine(s.split(","))).filter(x -> x != null).collect(Collectors.toList());

//		CSVFileReader csvFileReader = new CSVFileReader();

//		List<String[]> list = csvFileReader.read(fullFileName, COMMA_SEPERATOR, null);
//
//		List<Map<String, Object>> outputList = list.stream().map(x -> {
//
//			return parseLine(x);
//
//		}).filter(x -> x != null).collect(Collectors.toList());

		return outputList;
	}

	private Map<String, Object> parseLine(final String[] x) {

		Map<String, Object> map = null;
		final List<Integer> columns = COLUMN_LIST_STOOP;
		final String dateFormat = DATE_FORMAT_STOOP;
		final int intervalPosition = 1;
		final int timePosition = 3;
		Calendar calendar = getCalendar(dateFormat, x[columns.get(0)]);

		if (calendar != null) {
			int year = calendar.get(Calendar.YEAR);
			int dayOfYear = calendar.get(Calendar.DAY_OF_YEAR);
			int weekOfYear = calendar.get(Calendar.WEEK_OF_YEAR);

			map = new TreeMap<String, Object>();

			map.put(mapKey.getInterval(), new String(x[intervalPosition]));
			String tempSymbol = x[0];
			if (tempSymbol != null && tempSymbol.endsWith(STOOQ_TICKER_SUFFIX)) {
				tempSymbol = tempSymbol.substring(0, tempSymbol.length() - 3);
			}
			map.put(mapKey.getTicker(), tempSymbol);
			map.put(mapKey.getSymbol(), tempSymbol);

			map.put(mapKey.getDate(), new String(String.format(OUTPUT_DATE_FORMAT, calendar)));
			map.put(mapKey.getTime(), new String(x[timePosition]));
			map.put(mapKey.getYear(), year);
			map.put(mapKey.getMonth(), calendar.get(Calendar.MONTH) + 1);
			map.put(mapKey.getDay(), calendar.get(Calendar.DATE));
			map.put(mapKey.getDayOfYear(), dayOfYear);
			map.put(mapKey.getWeekOfYear(), weekOfYear);
			map.put(mapKey.getDayOfWeek(), calendar.get(Calendar.DAY_OF_WEEK));
			if (weekOfYear == 1 && ((year % 4 != 0 && dayOfYear >= 362)
					|| (((year % 4 == 0 && year % 100 != 0) || year % 400 == 0) && dayOfYear >= 363))) {
				map.put(mapKey.getYearForWeek(), year + 1);
			} else {
				map.put(mapKey.getYearForWeek(), year);
			}
			map.put(mapKey.getOpen(), Double.parseDouble(x[columns.get(1)]));
			map.put(mapKey.getHigh(), Double.parseDouble(x[columns.get(2)]));
			map.put(mapKey.getLow(), Double.parseDouble(x[columns.get(3)]));
			map.put(mapKey.getClose(), Double.parseDouble(x[columns.get(4)]));
			map.put(mapKey.getAdjClose(), Double.parseDouble(x[columns.get(5)]));
			map.put(mapKey.getVolume(), Long.parseLong(x[columns.get(6)]));

			String temp = map.get(mapKey.getTicker()).toString();
			if (temp.endsWith(TICKER_SUFFIX_TORONTO_STOCK_EXCHANGE)
					|| temp.endsWith(TICKER_SUFFIX_TORONTO_STOCK_VENTURE_EXCHANGE)) {
				map.put(mapKey.getMarket(), MARKET_CANADA);

			} else {
				map.put(mapKey.getMarket(), MARKET_UNITED_STATE);
			}
		}

		return map;

	}

	private Calendar getCalendar(String dateFormat, String date) {

		Calendar calendar = null;
		Date sDate;
		try {
			sDate = new SimpleDateFormat(dateFormat).parse(date);
			calendar = Calendar.getInstance();
			calendar.setTime(sDate);
			calendar.set(Calendar.MILLISECOND, 0);
			calendar.set(Calendar.SECOND, 0);
			calendar.set(Calendar.MINUTE, 0);
			calendar.set(Calendar.HOUR, 0);
		} catch (ParseException e) {
			log.info(e.toString());
		}
		return calendar;

	}
}