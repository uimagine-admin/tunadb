# Use an official Golang image as the base
FROM golang:1.21-alpine

# Set the working directory
WORKDIR /app

# Copy the Go module files and download dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy the rest of the application
COPY . .

# Build the Go node
RUN go build -o node ./cmd/node/main.go

# Expose the gRPC port
EXPOSE 50051

# Run the node binary
CMD ["./node"]