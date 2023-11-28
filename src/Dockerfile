# syntax=docker/dockerfile:1

FROM --platform=linux/amd64  golang:1.21.3

WORKDIR /app

COPY go.mod ./


RUN go mod download

COPY . .

# Compile wc.go (with -race flag)
RUN go build -buildmode=plugin ./mrapps/wc.go



# Start Worker LOCAL (with -race flag)
# RUN go run -race -gcflags="all=-N -l" mrworker.go wc.so

ENV PORT=8080

EXPOSE 8080


CMD ["go", "run", "-race", "./main/mrcoordinator.go", "./main/pg-*.txt"]

# CMD ["go", "run", "-race", "-gcflags=all=-N -l", "./main/mrworker.go", "./main/wc.so"]

# CMD ["sh", "-c", "go run -race ./main/mrcoordinator.go ./main/pg-*.txt && go run -race -gcflags=all=-N -l ./main/mrworker.go ./main/wc.so"]
