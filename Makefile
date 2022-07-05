docker-image:
	docker build -f docker/rabbitmq_config.Dockerfile -t "rabbitmq-config:latest" .
	docker build -f docker/docker_install.Dockerfile -t "docker_install:latest" .
	docker build -f docker/nodes.Dockerfile -t "memes-nodes:latest" .
.PHONY: docker-image

docker-compose-up: docker-image
	docker-compose -f docker-compose.yaml up -d --build
.PHONY: docker-compose-up

docker-compose-down:
	docker-compose -f docker-compose.yaml stop
	docker-compose -f docker-compose.yaml down
.PHONY: docker-compose-down

docker-compose-logs:
	docker-compose -f docker-compose.yaml logs -f
.PHONY: docker-compose-logs