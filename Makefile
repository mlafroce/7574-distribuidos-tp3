docker-image:
	docker build -f docker/rabbitmq_config.Dockerfile -t "rabbitmq-config:latest" .
	docker build -f docker/docker_install.Dockerfile -t "docker_install:latest" .
	docker build -f docker/nodes.Dockerfile -t "memes-nodes:latest" .
.PHONY: docker-image

docker-compose-up: docker-image
	docker-compose -f docker-compose.yaml up -d --build
.PHONY: docker-compose-up

docker-compose-down:
	docker-compose -f docker-compose.yaml stop -t 10 task_management_0 &
	docker-compose -f docker-compose.yaml stop -t 10 task_management_1 &
	docker-compose -f docker-compose.yaml stop -t 10 task_management_2 &
	docker-compose -f docker-compose.yaml stop -t 10 task_management_3
	docker-compose -f docker-compose.yaml stop -t 5 client
	docker-compose -f docker-compose.yaml stop -t 5 server
	docker-compose -f docker-compose.yaml stop -t 5 post_producer
	docker-compose -f docker-compose.yaml stop -t 5 comment_producer
	docker-compose -f docker-compose.yaml stop -t 5 best_meme_filter
	docker-compose -f docker-compose.yaml stop -t 5 comment_college_filter
	docker-compose -f docker-compose.yaml stop -t 5 comment_sentiment_extractor
	docker-compose -f docker-compose.yaml stop -t 5 mean_calculator
	docker-compose -f docker-compose.yaml stop -t 5 post_average_filter
	docker-compose -f docker-compose.yaml stop -t 5 post_college_filter
	docker-compose -f docker-compose.yaml stop -t 5 post_sentiment_calculator
	docker-compose -f docker-compose.yaml stop -t 5 post_sentiment_filter
	docker-compose -f docker-compose.yaml stop -t 5 score_extractor
	docker-compose -f docker-compose.yaml stop -t 5 url_extractor
	docker-compose -f docker-compose.yaml stop -t 10 rabbitmq

	docker-compose -f docker-compose.yaml down
.PHONY: docker-compose-down

docker-compose-logs:
	docker-compose -f docker-compose.yaml logs -f
.PHONY: docker-compose-logs