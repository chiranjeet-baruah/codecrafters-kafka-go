package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"os"
)

type ApiVersionsRequest struct {
	Length        int32
	ApiKey        int16
	ApiVersion    int16
	CorrelationID int32
	ClientID      string
}

type ApiVersionsResponse struct {
	Length        int32
	CorrelationID int32
	ErrorCode     int16
	ApiKeys       []ApiKey
}

type ApiKey struct {
	ApiKey     int16
	MinVersion int16
	MaxVersion int16
}

func main() {
	fmt.Println("Starting Kafka server...")

	listener, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}
	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		fmt.Println("Error reading:", err)
		return
	}

	data := buf[:n]
	reader := bytes.NewReader(data)

	var request ApiVersionsRequest
	binary.Read(reader, binary.BigEndian, &request.Length)
	binary.Read(reader, binary.BigEndian, &request.ApiKey)
	binary.Read(reader, binary.BigEndian, &request.ApiVersion)
	binary.Read(reader, binary.BigEndian, &request.CorrelationID)

	// Prepare response
	response := ApiVersionsResponse{
		CorrelationID: request.CorrelationID,
	}

	// Serialize response
	var responseBuf bytes.Buffer
	binary.Write(&responseBuf, binary.BigEndian, response.CorrelationID)

	// Send complete response
	conn.Write(responseBuf.Bytes())
}
