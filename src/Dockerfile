# 1: Build the Go binary
FROM --platform=linux/amd64 golang:1.16

# the working directory inside the container
WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

# the source code into the container
COPY . ./

# the Go binary
RUN CGO_ENABLED=0 GOOS=linux go build -buildmode=plugin -gcflags="all=-N -l" mrapps/wc.go


EXPOSE 1234

COPY .aws /root/.aws

#Command to run the executable
#CMD ["./mrcoordinator"]

#CMD ["/bin/sh", "-c", "go run ./main/mrcoordinator.go ./main/pg-*.txt"]
CMD ["/bin/sh", "-c", "go run ./main/mrcoordinator.go ./main/pg-*.txt"]



