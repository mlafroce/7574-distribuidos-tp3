FROM activatedgeek/rabbitmqadmin

COPY scripts.sh /var

ENTRYPOINT ["sh"]

CMD ["/var/scripts.sh"]
