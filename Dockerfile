# Stage 1: Compile the binary in a containerized Golang environment
#
FROM golang:1.24.1 as build
# Copy the source files from the host
COPY . /src
# Set the working directory to the same place we copied the code
WORKDIR /src
# Build the binary!
RUN CGO_ENABLED=0 GOOS=linux go build -o kvs
# Stage 2: Build the Key-Value Store image proper
#
FROM scratch
# Use a "scratch" image, which contains no distribution files
# Copy the binary from the build container
COPY --from=build /src/kvs .
# If you're using TLS, copy the .pem files too
COPY --from=build /src/*.pem .
# Tell Docker we'll be using port 8080
EXPOSE 8080
# Tell Docker to execute this command on a "docker run"
CMD ["/kvs"]