FROM golang:1.17

WORKDIR /usr/src/app

COPY go.mod .
COPY go.sum .

RUN go mod download

COPY . .

RUN go build -o ./out/main ./cmd/kio

CMD ["./out/main"]