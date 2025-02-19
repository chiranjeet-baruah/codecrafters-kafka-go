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

type ApiKeyVersion struct {
	ApiKey     int16
	MinVersion int16
	MaxVersion int16
}

type ApiVersionsResponse struct {
	MessageSize    int32
	CorrelationID  int32
	ErrorCode      uint16
	ApiKeyCount    int8
	ApiKeys        []ApiKeyVersion
	ThrottleTimeMs int32
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

	buf := make([]byte, 4096)
	n, err := conn.Read(buf)
	if err != nil {
		fmt.Println("Error reading:", err)
		return
	}

	data := buf[:n]
	reader := bytes.NewReader(data)

	var request ApiVersionsRequest
	// Read basic request fields
	if err := binary.Read(reader, binary.BigEndian, &request.MessageSize); err != nil {
		fmt.Println("Error reading MessageSize:", err)
		return
	}
	if err := binary.Read(reader, binary.BigEndian, &request.RequestApiKey); err != nil {
		fmt.Println("Error reading RequestApiKey:", err)
		return
	}
	if err := binary.Read(reader, binary.BigEndian, &request.RequestApiVersion); err != nil {
		fmt.Println("Error reading RequestApiVersion:", err)
		return
	}
	if err := binary.Read(reader, binary.BigEndian, &request.CorrelationID); err != nil {
		fmt.Println("Error reading CorrelationID:", err)
		return
	}

	// Prepare response with supported API key
	responseKeys := []ApiKeyVersion{
		{
			ApiKey:     18,
			MinVersion: 0,
			MaxVersion: 4,
		},
		{
			ApiKey:     3,
			MinVersion: 0,
			MaxVersion: 4,
		},
		{
			ApiKey:     5,
			MinVersion: 0,
			MaxVersion: 4,
		},
	}

	response := ApiVersionsResponse{
		CorrelationID:  request.CorrelationID,
		ApiKeyCount:    int8(len(responseKeys) + 1),
		ApiKeys:        responseKeys,
		ThrottleTimeMs: 0,
	}

	// If version is out of desired range, set an error code
	if request.RequestApiVersion < 0 || request.RequestApiVersion > 4 {
		response.ErrorCode = 35
	} else {
		response.ErrorCode = 0
	}

	// Serialize the response
	var responseBuf bytes.Buffer

	// 1) Correlation ID
	binary.Write(&responseBuf, binary.BigEndian, response.CorrelationID)
	// 2) Error code
	binary.Write(&responseBuf, binary.BigEndian, response.ErrorCode)
	// 3) ApiKeyCount
	binary.Write(&responseBuf, binary.BigEndian, response.ApiKeyCount)

	// 4) Api keys
	for _, key := range response.ApiKeys {
		binary.Write(&responseBuf, binary.BigEndian, key.ApiKey)
		binary.Write(&responseBuf, binary.BigEndian, key.MinVersion)
		binary.Write(&responseBuf, binary.BigEndian, key.MaxVersion)
		// Add tag buffer after each api key
		binary.Write(&responseBuf, binary.BigEndian, byte(0))
	}

	// 5) ThrottleTimeMs
	binary.Write(&responseBuf, binary.BigEndian, response.ThrottleTimeMs)

	// 6) Tag Buffer
	binary.Write(&responseBuf, binary.BigEndian, byte(0))

	// Total size of the response body
	response.MessageSize = int32(responseBuf.Len())

	// Prepend MessageSize to response
	finalBuf := &bytes.Buffer{}
	binary.Write(finalBuf, binary.BigEndian, response.MessageSize)

	finalBuf.Write(responseBuf.Bytes())

	// // Print byte count for each part
	// fmt.Println("CorrelationID:", binary.Size(response.CorrelationID))
	// fmt.Println("ErrorCode:", binary.Size(response.ErrorCode))
	// fmt.Println("ApiKeyCount:", binary.Size(response.ApiKeyCount))
	// fmt.Println("ApiKeys:", binary.Size(response.ApiKeys))
	// fmt.Println("ThrottleTimeMs:", binary.Size(response.ThrottleTimeMs))

	// // Total byte count
	// fmt.Println("Total bytes:", finalBuf.Len())

	conn.Write(finalBuf.Bytes())
}
