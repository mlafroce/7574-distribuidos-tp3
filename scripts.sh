#!/bin/bash

RABBITMQ_HOST=rabbitmq

echo "Connecting to $RABBITMQ_HOST..."

while true
do
    RESULT_DECLARE_QUEUE_OK="queue declared"
    RESULT_DECLARE_QUEUE=$(rabbitmqadmin -H $RABBITMQ_HOST declare queue auto_delete=false durable=false name=tp2.posts.college_src)
    if [ "$RESULT_DECLARE_QUEUE" = "$RESULT_DECLARE_QUEUE_OK" ];
    then
        echo "Connected to $RABBITMQ_HOST successfully!"
        break
    else
        echo "Server at $RABBITMQ_HOST not available yet, sleeping..."
        sleep 1
    fi
done


QUEUES="tp2.posts.url_src tp2.posts.urls tp2.posts.urls.id tp2.posts.college_id tp2.posts.above_average tp2.posts.score_src tp2.posts.mean tp2.posts.mean.result tp2.posts.sentiment tp2.posts.sentiment.filtered tp2.posts.sentiment.best tp2.comments.sentiment_src tp2.comments.college_src tp2.results tp2.data.save"
for queue_name in ${QUEUES}
do
rabbitmqadmin -H $RABBITMQ_HOST declare queue auto_delete=false durable=false name=$queue_name
done;
rabbitmqadmin -H $RABBITMQ_HOST declare exchange type=fanout name=tp2.posts
rabbitmqadmin -H $RABBITMQ_HOST declare exchange type=fanout name=tp2.comments

rabbitmqadmin -H $RABBITMQ_HOST declare binding source=tp2.posts destination=tp2.posts.college_src
rabbitmqadmin -H $RABBITMQ_HOST declare binding source=tp2.posts destination=tp2.posts.score_src
rabbitmqadmin -H $RABBITMQ_HOST declare binding source=tp2.posts destination=tp2.posts.url_src
rabbitmqadmin -H $RABBITMQ_HOST declare binding source=tp2.comments destination=tp2.comments.sentiment_src
rabbitmqadmin -H $RABBITMQ_HOST declare binding source=tp2.comments destination=tp2.comments.college_src
