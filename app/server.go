package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"os"
)

type ApiVersionsRequest struct {
	MessageSize       int32
	RequestApiKey     int16
	RequestApiVersion int16
	CorrelationID     int32
}

type ApiVersionsResponse struct {
	MessageSize   int32
	CorrelationID int32
	ErrorCode     int16
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
	binary.Read(reader, binary.BigEndian, &request.MessageSize)
	binary.Read(reader, binary.BigEndian, &request.RequestApiKey)
	binary.Read(reader, binary.BigEndian, &request.RequestApiVersion)
	binary.Read(reader, binary.BigEndian, &request.CorrelationID)

	// Prepare response
	response := ApiVersionsResponse{
		MessageSize:   10,
		CorrelationID: request.CorrelationID,
	}

	// If RequestApiVersion is not in 0 to 4, return UNSUPPORTED_VERSION
	if request.RequestApiVersion < 0 || request.RequestApiVersion > 4 {
		response.ErrorCode = 35
	} else {
		response.ErrorCode = 0
	}

	// Serialize response
	var responseBuf bytes.Buffer
	binary.Write(&responseBuf, binary.BigEndian, response.MessageSize)
	binary.Write(&responseBuf, binary.BigEndian, response.CorrelationID)
	binary.Write(&responseBuf, binary.BigEndian, response.ErrorCode)

	// Print response
	fmt.Println("Response:", response)

	// Send complete response
	conn.Write(responseBuf.Bytes())
}
