package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"os"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

type ApiRequest struct {
	CorrelationID uint32
}

type ApiResponse struct {
	MessageSize   uint32
	CorrelationID uint32
}

func main() {
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}
	conn, err := l.Accept()
	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
		os.Exit(1)
	}
	defer conn.Close()

	// Replace the JSON unmarshalling code with binary parsing
	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		fmt.Println("Error reading:", err.Error())
		os.Exit(1)
	}

	data := buf[:n]
	request := ApiRequest{}

	// Parse the binary format
	reader := bytes.NewReader(data)
	binary.Read(reader, binary.BigEndian, &request.CorrelationID)

	// Send the response
	response := ApiResponse{
		MessageSize:   0,
		CorrelationID: request.CorrelationID,
	}
	// response to bytes
	var responseBuf bytes.Buffer
	binary.Write(&responseBuf, binary.BigEndian, response.MessageSize)
	binary.Write(&responseBuf, binary.BigEndian, response.CorrelationID)
	conn.Write(responseBuf.Bytes())
}
