#!/bin/bash

while true
do
    RESULT_DECLARE_QUEUE_OK="queue declared"
    RESULT_DECLARE_QUEUE=$(rabbitmqadmin -H rabbitmq declare queue auto_delete=false durable=false name=tp2.posts.college_src)
    if [ "$RESULT_DECLARE_QUEUE" = "$RESULT_DECLARE_QUEUE_OK" ];
    then
        break
    else
        sleep 1
    fi
done

rabbitmqadmin -H rabbitmq declare queue auto_delete=false durable=false name=tp2.posts.url_src
rabbitmqadmin -H rabbitmq declare queue auto_delete=false durable=false name=tp2.posts.urls
rabbitmqadmin -H rabbitmq declare queue auto_delete=false durable=false name=tp2.posts.urls.id
rabbitmqadmin -H rabbitmq declare queue auto_delete=false durable=false name=tp2.posts.college_id
rabbitmqadmin -H rabbitmq declare queue auto_delete=false durable=false name=tp2.posts.above_average
rabbitmqadmin -H rabbitmq declare queue auto_delete=false durable=false name=tp2.posts.score_src
rabbitmqadmin -H rabbitmq declare queue auto_delete=false durable=false name=tp2.posts.mean
rabbitmqadmin -H rabbitmq declare queue auto_delete=false durable=false name=tp2.posts.mean.result
rabbitmqadmin -H rabbitmq declare queue auto_delete=false durable=false name=tp2.posts.sentiment
rabbitmqadmin -H rabbitmq declare queue auto_delete=false durable=false name=tp2.posts.sentiment.filtered
rabbitmqadmin -H rabbitmq declare queue auto_delete=false durable=false name=tp2.posts.sentiment.best
rabbitmqadmin -H rabbitmq declare queue auto_delete=false durable=false name=tp2.comments.sentiment_src
rabbitmqadmin -H rabbitmq declare queue auto_delete=false durable=false name=tp2.comments.college_src
rabbitmqadmin -H rabbitmq declare queue auto_delete=false durable=false name=tp2.results
rabbitmqadmin -H rabbitmq declare queue auto_delete=false durable=false name=tp2.data.save
rabbitmqadmin -H rabbitmq declare exchange type=fanout name=tp2.posts
rabbitmqadmin -H rabbitmq declare exchange type=fanout name=tp2.comments

rabbitmqadmin -H rabbitmq declare binding source=tp2.posts destination=tp2.posts.college_src
rabbitmqadmin -H rabbitmq declare binding source=tp2.posts destination=tp2.posts.score_src
rabbitmqadmin -H rabbitmq declare binding source=tp2.posts destination=tp2.posts.url_src
rabbitmqadmin -H rabbitmq declare binding source=tp2.comments destination=tp2.comments.sentiment_src
rabbitmqadmin -H rabbitmq declare binding source=tp2.comments destination=tp2.comments.college_src
