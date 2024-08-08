FROM hub.byted.org/base/vnet-base-image:1.0.0.2
COPY gitconfig /etc/gitconfig
ARG CODEBASE
ENV GIT_SSH_COMMAND=$CODEBASE
ADD . /go/src/go-mysql-transfer
WORKDIR /go/src/go-mysql-transfer
RUN go build -o vnet-mysql-transfer

FROM hub.byted.org/base/vestack.debian-slim:bookworm-security
COPY --from=0 /go/src/go-mysql-transfer/vnet-mysql-transfer /usr/local/bin/
COPY --from=0 /go/src/go-mysql-transfer/app.yml /etc/vnet-mysql-transfer/
ENV TZ=Asia/Shanghai
CMD ["vnet-mysql-transfer","-stock"]
