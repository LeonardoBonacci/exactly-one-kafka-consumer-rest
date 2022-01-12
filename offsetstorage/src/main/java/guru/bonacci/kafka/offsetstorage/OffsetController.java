package guru.bonacci.kafka.offsetstorage;

import java.util.HashMap;
import java.util.Map;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@RestController
public class OffsetController {

	final Map<Integer, Long> offsetsByPartition = new HashMap<>();

	@GetMapping
	public Map<Integer, Long> getOffsets() {
		log.info("getOffsets");

		return offsetsByPartition;
	}

	@PostMapping
	public void endpoint(@RequestHeader("partition") Integer partition,
						 @RequestHeader("offset") Long offset,
						 @RequestBody String foo) {

		// atomically process request body and offset headers
		log.info("Header values: {}-{}", partition, offset);
		log.info("Request body: {}", foo);
		offsetsByPartition.put(partition, offset);
	}
}
