FROM golang:alpine AS builder
RUN apk update && apk add --no-cache git binutils
WORKDIR $GOPATH/src/moonsnap-downloadoor
COPY . .
RUN go get -d -v
RUN go build -o /go/bin/moonsnap-downloadoor
RUN strip /go/bin/moonsnap-downloadoor

FROM scratch
COPY --from=builder /go/bin/moonsnap-downloadoor /go/bin/moonsnap-downloadoor
COPY --from=builder /go/src/moonsnap-downloadoor/fonts/block.flf /go/bin/
COPY --from=builder /etc/ssl/certs/ /etc/ssl/certs/
ENTRYPOINT ["/go/bin/moonsnap-downloadoor"]
