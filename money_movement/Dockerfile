FROM golang:latest AS build

WORKDIR /app

COPY go.mod ./

RUN go mod tidy

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o money_movement ./cmd/money_movement

FROM alpine:latest

WORKDIR /app

COPY --from=build /app/money_movement .

ENTRYPOINT [ "./money_movement" ]