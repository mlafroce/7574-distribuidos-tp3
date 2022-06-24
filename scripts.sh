rabbitmqadmin -H 172.21.0.2 declare queue auto_delete=false durable=false name=tp2.posts.college_src
rabbitmqadmin -H 172.21.0.2 declare queue auto_delete=false durable=false name=tp2.posts.url_src
rabbitmqadmin -H 172.21.0.2 declare queue auto_delete=false durable=false name=tp2.posts.urls
rabbitmqadmin -H 172.21.0.2 declare queue auto_delete=false durable=false name=tp2.posts.urls.id
rabbitmqadmin -H 172.21.0.2 declare queue auto_delete=false durable=false name=tp2.posts.college_id
rabbitmqadmin -H 172.21.0.2 declare queue auto_delete=false durable=false name=tp2.posts.above_average
rabbitmqadmin -H 172.21.0.2 declare queue auto_delete=false durable=false name=tp2.posts.score_src
rabbitmqadmin -H 172.21.0.2 declare queue auto_delete=false durable=false name=tp2.posts.mean
rabbitmqadmin -H 172.21.0.2 declare queue auto_delete=false durable=false name=tp2.posts.mean.result
rabbitmqadmin -H 172.21.0.2 declare queue auto_delete=false durable=false name=tp2.posts.sentiment
rabbitmqadmin -H 172.21.0.2 declare queue auto_delete=false durable=false name=tp2.posts.sentiment.filtered
rabbitmqadmin -H 172.21.0.2 declare queue auto_delete=false durable=false name=tp2.posts.sentiment.best
rabbitmqadmin -H 172.21.0.2 declare queue auto_delete=false durable=false name=tp2.comments.sentiment_src
rabbitmqadmin -H 172.21.0.2 declare queue auto_delete=false durable=false name=tp2.comments.college_src
rabbitmqadmin -H 172.21.0.2 declare queue auto_delete=false durable=false name=tp2.results
rabbitmqadmin -H 172.21.0.2 declare exchange type=fanout name=tp2.posts
rabbitmqadmin -H 172.21.0.2 declare exchange type=fanout name=tp2.comments

rabbitmqadmin -H 172.21.0.2 declare binding source=tp2.posts destination=tp2.posts.college_src
rabbitmqadmin -H 172.21.0.2 declare binding source=tp2.posts destination=tp2.posts.score_src
rabbitmqadmin -H 172.21.0.2 declare binding source=tp2.posts destination=tp2.posts.url_src
rabbitmqadmin -H 172.21.0.2 declare binding source=tp2.comments destination=tp2.comments.sentiment_src
rabbitmqadmin -H 172.21.0.2 declare binding source=tp2.comments destination=tp2.comments.college_src
