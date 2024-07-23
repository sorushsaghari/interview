
FROM golang:1.22 AS build

# Set the current working directory inside the container.
WORKDIR /app

# Copy the go mod and sum files.
COPY go.mod go.sum ./

# Download dependencies.
RUN go mod download

# Copy the source code.
COPY . .

# Build the Go app.
RUN go build -o /app/main .

# Use a minimal image to run the Go app.
# This is based on Alpine and is around 5MB.
FROM alpine:3.14

# Install necessary packages.
RUN apk add --no-cache ca-certificates

# Set the current working directory inside the container.
WORKDIR /app

# Copy the binary from the build stage.
COPY --from=build /app/main /app/main

# Copy the configuration file.
COPY config.yaml /app/config.yaml

# Expose the port the app runs on.
EXPOSE 1323

# Run the binary.
CMD ["/app/main"]
