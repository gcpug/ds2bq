FROM alpine:3.7
RUN apk add --no-cache ca-certificates
RUN mkdir /lib64 && ln -s /lib/libc.musl-x86_64.so.1 /lib64/ld-linux-x86-64.so.2
COPY ./ds2bq /ds2bq
ENTRYPOINT ["/ds2bq"]