.PHONY: kafka topology client

kafka:
	docker-compose down -v && docker-compose up

topology:
	./gradlew topology:clean topology:build topology:run

client:
	./gradlew client:build client:installDist
	./client/build/install/client/bin/client

reset:
	rm -rf /tmp/statestoresadness/