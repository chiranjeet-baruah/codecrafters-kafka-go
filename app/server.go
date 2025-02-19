package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
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

	reader := bufio.NewReader(conn)
	for {
		// Read the first 4 bytes for MessageSize
		var messageSize int32
		if err := binary.Read(reader, binary.BigEndian, &messageSize); err != nil {
			// If error reading, assume connection closed or error occurred
			if err == io.EOF {
				fmt.Println("Connection closed by client")
			} else {
				fmt.Println("Error reading MessageSize:", err)
			}
			return
		}

		// Read the remaining bytes for the complete request
		requestBody := make([]byte, messageSize)
		if _, err := io.ReadFull(reader, requestBody); err != nil {
			fmt.Println("Error reading request body:", err)
			return
		}

		// Prepend the MessageSize read earlier to reconstruct the full request buffer
		completeBuffer := &bytes.Buffer{}
		binary.Write(completeBuffer, binary.BigEndian, messageSize)
		completeBuffer.Write(requestBody)

		requestReader := bytes.NewReader(completeBuffer.Bytes())
		var request ApiVersionsRequest
		if err := binary.Read(requestReader, binary.BigEndian, &request.MessageSize); err != nil {
			fmt.Println("Error reading MessageSize from buffer:", err)
			continue
		}
		if err := binary.Read(requestReader, binary.BigEndian, &request.RequestApiKey); err != nil {
			fmt.Println("Error reading RequestApiKey:", err)
			continue
		}
		if err := binary.Read(requestReader, binary.BigEndian, &request.RequestApiVersion); err != nil {
			fmt.Println("Error reading RequestApiVersion:", err)
			continue
		}
		if err := binary.Read(requestReader, binary.BigEndian, &request.CorrelationID); err != nil {
			fmt.Println("Error reading CorrelationID:", err)
			continue
		}

		// Prepare response with supported API keys
		responseKeys := []ApiKeyVersion{
			{ApiKey: 18, MinVersion: 0, MaxVersion: 4},
			{ApiKey: 3, MinVersion: 0, MaxVersion: 4},
			{ApiKey: 5, MinVersion: 0, MaxVersion: 4},
		}

		response := ApiVersionsResponse{
			CorrelationID:  request.CorrelationID,
			ApiKeyCount:    int8(len(responseKeys) + 1),
			ApiKeys:        responseKeys,
			ThrottleTimeMs: 0,
		}

		// Set error code if RequestApiVersion is out of desired range
		if request.RequestApiVersion < 0 || request.RequestApiVersion > 4 {
			response.ErrorCode = 35
		} else {
			response.ErrorCode = 0
		}

		// Serialize the response
		var responseBuf bytes.Buffer
		binary.Write(&responseBuf, binary.BigEndian, response.CorrelationID)
		binary.Write(&responseBuf, binary.BigEndian, response.ErrorCode)
		binary.Write(&responseBuf, binary.BigEndian, response.ApiKeyCount)

		for _, key := range response.ApiKeys {
			binary.Write(&responseBuf, binary.BigEndian, key.ApiKey)
			binary.Write(&responseBuf, binary.BigEndian, key.MinVersion)
			binary.Write(&responseBuf, binary.BigEndian, key.MaxVersion)
			// Write tag buffer for each api key
			binary.Write(&responseBuf, binary.BigEndian, byte(0))
		}

		binary.Write(&responseBuf, binary.BigEndian, response.ThrottleTimeMs)
		binary.Write(&responseBuf, binary.BigEndian, byte(0))

		// Set MessageSize to total bytes of response body
		response.MessageSize = int32(responseBuf.Len())

		// Prepend MessageSize to the response
		finalBuf := &bytes.Buffer{}
		binary.Write(finalBuf, binary.BigEndian, response.MessageSize)
		finalBuf.Write(responseBuf.Bytes())

		if _, err := conn.Write(finalBuf.Bytes()); err != nil {
			fmt.Println("Error writing response:", err)
			return
		}
	}
}
